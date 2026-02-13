"""Tenant-scoped concurrency controls."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, Optional

from opentelemetry import trace

from common.config.env import get_env_float, get_env_int
from common.observability.metrics import agent_metrics


@dataclass(frozen=True)
class TenantLease:
    """Acquired tenant lease details."""

    tenant_id: int
    active_runs: int
    limit: int


class TenantConcurrencyLimitExceeded(RuntimeError):
    """Raised when a tenant exceeds configured concurrency."""

    def __init__(
        self,
        *,
        tenant_id: int,
        limit: int,
        active_runs: int,
        retry_after_seconds: float,
    ) -> None:
        """Capture structured concurrency-limit metadata for callers."""
        self.tenant_id = tenant_id
        self.limit = limit
        self.active_runs = active_runs
        self.retry_after_seconds = retry_after_seconds
        super().__init__("Tenant concurrency limit exceeded.")


@dataclass
class _TenantState:
    semaphore: asyncio.Semaphore
    active_runs: int = 0
    last_access_ts: float = 0.0


class TenantConcurrencyLimiter:
    """Per-tenant concurrency limiter with bounded in-memory state."""

    def __init__(
        self,
        *,
        per_tenant_limit: int,
        max_tracked_tenants: int,
        idle_ttl_seconds: float,
        retry_after_seconds: float,
        span_active_attribute: str = "tenant.active_runs",
        span_limit_attribute: str = "tenant.limit",
        span_limit_exceeded_attribute: str = "tenant.limit_exceeded",
        active_runs_histogram_name: str = "agent.tenant.active_runs",
        limit_exceeded_counter_name: str = "agent.tenant.limit_exceeded_total",
        metrics_scope: str = "agent_run",
    ) -> None:
        """Initialize limiter limits and bounded tenant state controls."""
        self._per_tenant_limit = max(1, int(per_tenant_limit))
        self._max_tracked_tenants = max(1, int(max_tracked_tenants))
        self._idle_ttl_seconds = max(0.0, float(idle_ttl_seconds))
        self._retry_after_seconds = max(0.1, float(retry_after_seconds))
        self._span_active_attribute = str(span_active_attribute or "tenant.active_runs")
        self._span_limit_attribute = str(span_limit_attribute or "tenant.limit")
        self._span_limit_exceeded_attribute = str(
            span_limit_exceeded_attribute or "tenant.limit_exceeded"
        )
        self._active_runs_histogram_name = str(
            active_runs_histogram_name or "agent.tenant.active_runs"
        )
        self._limit_exceeded_counter_name = str(
            limit_exceeded_counter_name or "agent.tenant.limit_exceeded_total"
        )
        self._metrics_scope = str(metrics_scope or "agent_run")
        self._state_lock = asyncio.Lock()
        self._tenant_states: OrderedDict[int, _TenantState] = OrderedDict()

    async def _get_or_create_state(self, tenant_id: int) -> _TenantState:
        now = time.monotonic()
        async with self._state_lock:
            state = self._tenant_states.get(tenant_id)
            if state is None:
                state = _TenantState(
                    semaphore=asyncio.Semaphore(self._per_tenant_limit),
                    active_runs=0,
                    last_access_ts=now,
                )
                self._tenant_states[tenant_id] = state
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            self._prune_idle_locked(now)
            return state

    def _prune_idle_locked(self, now: float) -> None:
        if self._idle_ttl_seconds > 0:
            expired = [
                tenant_id
                for tenant_id, state in self._tenant_states.items()
                if state.active_runs == 0 and (now - state.last_access_ts) >= self._idle_ttl_seconds
            ]
            for tenant_id in expired:
                self._tenant_states.pop(tenant_id, None)

        if len(self._tenant_states) <= self._max_tracked_tenants:
            return

        for tenant_id, state in list(self._tenant_states.items()):
            if len(self._tenant_states) <= self._max_tracked_tenants:
                break
            if state.active_runs == 0:
                self._tenant_states.pop(tenant_id, None)

    def _record_telemetry(self, *, active_runs: int, limit_exceeded: bool) -> None:
        span = trace.get_current_span()
        if span is not None and span.is_recording():
            span.set_attribute(self._span_active_attribute, int(active_runs))
            span.set_attribute(self._span_limit_attribute, int(self._per_tenant_limit))
            span.set_attribute(self._span_limit_exceeded_attribute, bool(limit_exceeded))
        agent_metrics.record_histogram(
            self._active_runs_histogram_name,
            float(active_runs),
            description="Active in-flight agent runs for current tenant",
            attributes={"scope": self._metrics_scope},
        )
        if limit_exceeded:
            agent_metrics.add_counter(
                self._limit_exceeded_counter_name,
                attributes={"scope": self._metrics_scope},
                description="Count of tenant concurrency limit rejections",
            )

    async def _mark_acquired(self, tenant_id: int, state: _TenantState) -> int:
        now = time.monotonic()
        async with self._state_lock:
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            return state.active_runs

    async def _reserve_slot(self, tenant_id: int, state: _TenantState) -> int | None:
        now = time.monotonic()
        async with self._state_lock:
            if state.active_runs >= self._per_tenant_limit:
                state.last_access_ts = now
                self._tenant_states.move_to_end(tenant_id)
                from agent.audit import AuditEventType, emit_audit_event
                from common.models.error_metadata import ErrorCategory

                emit_audit_event(
                    AuditEventType.TENANT_CONCURRENCY_BLOCK,
                    tenant_id=tenant_id,
                    error_category=ErrorCategory.RESOURCE_EXHAUSTED,
                    metadata={
                        "scope": self._metrics_scope,
                        "active_runs": int(state.active_runs),
                        "limit": int(self._per_tenant_limit),
                    },
                )
                return None
            state.active_runs += 1
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            return state.active_runs

    async def _rollback_reservation(self, tenant_id: int, state: _TenantState) -> int:
        now = time.monotonic()
        async with self._state_lock:
            state.active_runs = max(0, state.active_runs - 1)
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            self._prune_idle_locked(now)
            return state.active_runs

    async def _mark_released(self, tenant_id: int, state: _TenantState) -> int:
        now = time.monotonic()
        async with self._state_lock:
            state.active_runs = max(0, state.active_runs - 1)
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            self._prune_idle_locked(now)
            return state.active_runs

    @asynccontextmanager
    async def acquire(self, tenant_id: int) -> AsyncIterator[TenantLease]:
        """Acquire a non-blocking tenant lease or raise limit exceeded."""
        state = await self._get_or_create_state(tenant_id)
        reserved_active_runs = await self._reserve_slot(tenant_id, state)
        if reserved_active_runs is None:
            active_runs = int(state.active_runs)
            self._record_telemetry(active_runs=active_runs, limit_exceeded=True)
            raise TenantConcurrencyLimitExceeded(
                tenant_id=tenant_id,
                limit=self._per_tenant_limit,
                active_runs=active_runs,
                retry_after_seconds=self._retry_after_seconds,
            )

        acquired = False
        try:
            await state.semaphore.acquire()
            acquired = True
        except Exception:
            active_runs = await self._rollback_reservation(tenant_id, state)
            self._record_telemetry(active_runs=active_runs, limit_exceeded=False)
            raise

        active_runs = await self._mark_acquired(tenant_id, state)
        self._record_telemetry(active_runs=active_runs, limit_exceeded=False)
        try:
            yield TenantLease(
                tenant_id=tenant_id,
                active_runs=active_runs,
                limit=self._per_tenant_limit,
            )
        finally:
            if acquired:
                await asyncio.shield(self._release(tenant_id, state))

    async def _release(self, tenant_id: int, state: _TenantState) -> None:
        state.semaphore.release()
        active_runs = await self._mark_released(tenant_id, state)
        self._record_telemetry(active_runs=active_runs, limit_exceeded=False)


_AGENT_RUN_LIMITER: Optional[TenantConcurrencyLimiter] = None
_MCP_TOOL_LIMITER: Optional[TenantConcurrencyLimiter] = None


def _safe_env_int(name: str, default: int, minimum: int) -> int:
    try:
        value = get_env_int(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, int(value))


def _safe_env_float(name: str, default: float, minimum: float) -> float:
    try:
        value = get_env_float(name, default)
    except ValueError:
        value = default
    if value is None:
        value = default
    return max(minimum, float(value))


def get_agent_run_tenant_limiter() -> TenantConcurrencyLimiter:
    """Return singleton tenant concurrency limiter for agent runs."""
    global _AGENT_RUN_LIMITER
    if _AGENT_RUN_LIMITER is None:
        _AGENT_RUN_LIMITER = TenantConcurrencyLimiter(
            per_tenant_limit=_safe_env_int("AGENT_TENANT_MAX_CONCURRENT_RUNS", 3, minimum=1),
            max_tracked_tenants=_safe_env_int("AGENT_TENANT_LIMITER_MAX_TENANTS", 2000, minimum=1),
            idle_ttl_seconds=_safe_env_float(
                "AGENT_TENANT_LIMITER_IDLE_TTL_SECONDS", 900.0, minimum=0.0
            ),
            retry_after_seconds=_safe_env_float(
                "AGENT_TENANT_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1
            ),
        )
    return _AGENT_RUN_LIMITER


def reset_agent_run_tenant_limiter() -> None:
    """Reset singleton limiter (test helper)."""
    global _AGENT_RUN_LIMITER
    _AGENT_RUN_LIMITER = None


def get_mcp_tool_tenant_limiter() -> TenantConcurrencyLimiter:
    """Return singleton tenant concurrency limiter for MCP tool invocations."""
    global _MCP_TOOL_LIMITER
    if _MCP_TOOL_LIMITER is None:
        _MCP_TOOL_LIMITER = TenantConcurrencyLimiter(
            per_tenant_limit=_safe_env_int("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", 3, minimum=1),
            max_tracked_tenants=_safe_env_int("MCP_TENANT_LIMITER_MAX_TENANTS", 2000, minimum=1),
            idle_ttl_seconds=_safe_env_float(
                "MCP_TENANT_LIMITER_IDLE_TTL_SECONDS", 900.0, minimum=0.0
            ),
            retry_after_seconds=_safe_env_float(
                "MCP_TENANT_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1
            ),
            span_active_attribute="tenant.active_tool_calls",
            span_limit_attribute="tenant.limit",
            span_limit_exceeded_attribute="tenant.limit_exceeded",
            active_runs_histogram_name="mcp.tenant.active_tool_calls",
            limit_exceeded_counter_name="mcp.tenant.limit_exceeded.total",
            metrics_scope="mcp_tool",
        )
    return _MCP_TOOL_LIMITER


def reset_mcp_tool_tenant_limiter() -> None:
    """Reset singleton limiter for MCP tool invocations (test helper)."""
    global _MCP_TOOL_LIMITER
    _MCP_TOOL_LIMITER = None

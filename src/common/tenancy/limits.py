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
        limit_kind: str = "concurrency",
        tokens_remaining: Optional[float] = None,
    ) -> None:
        """Capture structured concurrency-limit metadata for callers."""
        self.tenant_id = tenant_id
        self.limit = limit
        self.active_runs = active_runs
        self.retry_after_seconds = retry_after_seconds
        self.limit_kind = str(limit_kind or "concurrency")
        self.tokens_remaining = float(tokens_remaining) if tokens_remaining is not None else None
        super().__init__("Tenant concurrency limit exceeded.")


@dataclass
class _TenantState:
    semaphore: asyncio.Semaphore
    active_runs: int = 0
    last_access_ts: float = 0.0
    tokens: float = 0.0
    last_refill_ts: float = 0.0


@dataclass(frozen=True)
class _ReserveOutcome:
    active_runs: int | None
    effective_limit: int
    warm_start_active: bool
    retry_after_seconds: Optional[float] = None
    tokens_remaining: Optional[float] = None
    limit_kind: str = "concurrency"


class TenantConcurrencyLimiter:
    """Per-tenant concurrency limiter with bounded in-memory state."""

    def __init__(
        self,
        *,
        per_tenant_limit: int,
        max_tracked_tenants: int,
        idle_ttl_seconds: float,
        retry_after_seconds: float,
        refill_rate: float = 0.0,
        burst_capacity: Optional[int] = None,
        warm_start_cooldown_seconds: float = 0.0,
        warm_start_per_tenant_limit: Optional[int] = None,
        warm_start_burst_capacity: Optional[int] = None,
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
        self._refill_rate = max(0.0, float(refill_rate))
        resolved_burst_capacity = burst_capacity
        if resolved_burst_capacity is None:
            resolved_burst_capacity = self._per_tenant_limit * 4
        self._burst_capacity = max(1, int(resolved_burst_capacity))
        self._warm_start_cooldown_seconds = max(0.0, float(warm_start_cooldown_seconds))
        resolved_warm_start_limit = (
            self._per_tenant_limit
            if warm_start_per_tenant_limit is None
            else int(warm_start_per_tenant_limit)
        )
        self._warm_start_per_tenant_limit = max(
            1,
            min(int(self._per_tenant_limit), int(resolved_warm_start_limit)),
        )
        resolved_warm_start_burst = (
            self._burst_capacity
            if warm_start_burst_capacity is None
            else int(warm_start_burst_capacity)
        )
        self._warm_start_burst_capacity = max(
            1,
            min(int(self._burst_capacity), int(resolved_warm_start_burst)),
        )
        self._created_at_monotonic = time.monotonic()
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

    def _audit_source_for_scope(self) -> str:
        if self._metrics_scope == "mcp_tool":
            return "mcp"
        return "agent"

    def _warm_start_active(self, now: float) -> bool:
        if self._warm_start_cooldown_seconds <= 0:
            return False
        return (now - self._created_at_monotonic) < self._warm_start_cooldown_seconds

    def _effective_per_tenant_limit(self, now: float) -> int:
        if self._warm_start_active(now):
            return int(self._warm_start_per_tenant_limit)
        return int(self._per_tenant_limit)

    def _effective_burst_capacity(self, now: float) -> int:
        if self._warm_start_active(now):
            return int(self._warm_start_burst_capacity)
        return int(self._burst_capacity)

    async def _get_or_create_state(self, tenant_id: int) -> _TenantState:
        now = time.monotonic()
        async with self._state_lock:
            state = self._tenant_states.get(tenant_id)
            if state is None:
                state = _TenantState(
                    semaphore=asyncio.Semaphore(self._per_tenant_limit),
                    active_runs=0,
                    last_access_ts=now,
                    tokens=float(self._effective_burst_capacity(now)),
                    last_refill_ts=now,
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

    def _record_telemetry(
        self,
        *,
        active_runs: int,
        limit_exceeded: bool,
        effective_limit: Optional[int] = None,
        warm_start_active: Optional[bool] = None,
        tokens_remaining: Optional[float] = None,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        now = time.monotonic()
        resolved_limit = (
            int(self._effective_per_tenant_limit(now))
            if effective_limit is None
            else max(1, int(effective_limit))
        )
        resolved_warm_start_active = (
            bool(self._warm_start_active(now))
            if warm_start_active is None
            else bool(warm_start_active)
        )
        span = trace.get_current_span()
        normalized_tokens_remaining = (
            -1.0 if tokens_remaining is None else max(0.0, float(tokens_remaining))
        )
        normalized_retry_after = (
            0.0 if retry_after_seconds is None else max(0.0, float(retry_after_seconds))
        )
        if span is not None and span.is_recording():
            span.set_attribute(self._span_active_attribute, int(active_runs))
            span.set_attribute(self._span_limit_attribute, int(resolved_limit))
            span.set_attribute(self._span_limit_exceeded_attribute, bool(limit_exceeded))
            span.set_attribute("tenant.rate.tokens_remaining", normalized_tokens_remaining)
            span.set_attribute("tenant.rate.retry_after", normalized_retry_after)
            span.set_attribute("system.warm_start.active", resolved_warm_start_active)
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

    def _refill_tokens_locked(
        self, state: _TenantState, now: float, *, burst_capacity: int
    ) -> Optional[float]:
        if self._refill_rate <= 0:
            return None

        elapsed = max(0.0, float(now - state.last_refill_ts))
        if elapsed > 0:
            state.tokens = min(
                float(max(1, int(burst_capacity))),
                float(state.tokens) + (elapsed * self._refill_rate),
            )
            state.last_refill_ts = now
        return max(0.0, float(state.tokens))

    def _rate_limit_retry_after(self, tokens_remaining: float) -> float:
        if self._refill_rate <= 0:
            return self._retry_after_seconds
        deficit = max(0.0, 1.0 - max(0.0, float(tokens_remaining)))
        return max(0.1, deficit / self._refill_rate)

    async def _mark_acquired(self, tenant_id: int, state: _TenantState) -> int:
        now = time.monotonic()
        async with self._state_lock:
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            return state.active_runs

    async def _reserve_slot(self, tenant_id: int, state: _TenantState) -> _ReserveOutcome:
        now = time.monotonic()
        async with self._state_lock:
            effective_limit = self._effective_per_tenant_limit(now)
            effective_burst_capacity = self._effective_burst_capacity(now)
            warm_start_active = self._warm_start_active(now)
            tokens_remaining = self._refill_tokens_locked(
                state,
                now,
                burst_capacity=effective_burst_capacity,
            )
            if state.active_runs >= effective_limit:
                state.last_access_ts = now
                self._tenant_states.move_to_end(tenant_id)
                from agent.audit import AuditEventType, emit_audit_event
                from common.models.error_metadata import ErrorCategory

                emit_audit_event(
                    AuditEventType.TENANT_CONCURRENCY_LIMIT_EXCEEDED,
                    source=self._audit_source_for_scope(),
                    tenant_id=tenant_id,
                    error_category=ErrorCategory.LIMIT_EXCEEDED,
                    metadata={
                        "scope": self._metrics_scope,
                        "reason_code": "tenant_concurrency_limit_exceeded",
                        "decision": "reject",
                        "active_runs": int(state.active_runs),
                        "limit": int(effective_limit),
                        "warm_start": bool(warm_start_active),
                    },
                )
                return _ReserveOutcome(
                    active_runs=None,
                    effective_limit=effective_limit,
                    warm_start_active=warm_start_active,
                    retry_after_seconds=self._retry_after_seconds,
                    tokens_remaining=tokens_remaining,
                    limit_kind="concurrency",
                )

            if tokens_remaining is not None and tokens_remaining < 1.0:
                state.last_access_ts = now
                self._tenant_states.move_to_end(tenant_id)
                retry_after = self._rate_limit_retry_after(tokens_remaining)
                from agent.audit import AuditEventType, emit_audit_event
                from common.models.error_metadata import ErrorCategory

                emit_audit_event(
                    AuditEventType.TENANT_RATE_LIMITED,
                    source=self._audit_source_for_scope(),
                    tenant_id=tenant_id,
                    error_category=ErrorCategory.LIMIT_EXCEEDED,
                    metadata={
                        "scope": self._metrics_scope,
                        "reason_code": "tenant_rate_limited",
                        "decision": "reject",
                        "active_runs": int(state.active_runs),
                        "limit": int(effective_limit),
                        "burst_capacity": int(effective_burst_capacity),
                        "warm_start": bool(warm_start_active),
                        "retry_after_seconds": retry_after,
                    },
                )
                return _ReserveOutcome(
                    active_runs=None,
                    effective_limit=effective_limit,
                    warm_start_active=warm_start_active,
                    retry_after_seconds=retry_after,
                    tokens_remaining=tokens_remaining,
                    limit_kind="rate",
                )

            if tokens_remaining is not None:
                state.tokens = max(0.0, float(tokens_remaining) - 1.0)
                tokens_remaining = state.tokens
            state.active_runs += 1
            state.last_access_ts = now
            self._tenant_states.move_to_end(tenant_id)
            return _ReserveOutcome(
                active_runs=state.active_runs,
                effective_limit=effective_limit,
                warm_start_active=warm_start_active,
                tokens_remaining=tokens_remaining,
                limit_kind="concurrency",
            )

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
        reserve_outcome = await self._reserve_slot(tenant_id, state)
        if reserve_outcome.active_runs is None:
            active_runs = int(state.active_runs)
            self._record_telemetry(
                active_runs=active_runs,
                limit_exceeded=True,
                effective_limit=reserve_outcome.effective_limit,
                warm_start_active=reserve_outcome.warm_start_active,
                tokens_remaining=reserve_outcome.tokens_remaining,
                retry_after_seconds=reserve_outcome.retry_after_seconds,
            )
            raise TenantConcurrencyLimitExceeded(
                tenant_id=tenant_id,
                limit=reserve_outcome.effective_limit,
                active_runs=active_runs,
                retry_after_seconds=reserve_outcome.retry_after_seconds
                or self._retry_after_seconds,
                limit_kind=reserve_outcome.limit_kind,
                tokens_remaining=reserve_outcome.tokens_remaining,
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
        self._record_telemetry(
            active_runs=active_runs,
            limit_exceeded=False,
            effective_limit=reserve_outcome.effective_limit,
            warm_start_active=reserve_outcome.warm_start_active,
            tokens_remaining=reserve_outcome.tokens_remaining,
            retry_after_seconds=0.0,
        )
        try:
            yield TenantLease(
                tenant_id=tenant_id,
                active_runs=active_runs,
                limit=reserve_outcome.effective_limit,
            )
        finally:
            if acquired:
                await asyncio.shield(self._release(tenant_id, state))

    async def _release(self, tenant_id: int, state: _TenantState) -> None:
        state.semaphore.release()
        active_runs = await self._mark_released(tenant_id, state)
        tokens_remaining = max(0.0, float(state.tokens)) if self._refill_rate > 0 else None
        self._record_telemetry(
            active_runs=active_runs,
            limit_exceeded=False,
            tokens_remaining=tokens_remaining,
            retry_after_seconds=0.0,
        )


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
        per_tenant_limit = _safe_env_int("AGENT_TENANT_MAX_CONCURRENT_RUNS", 3, minimum=1)
        burst_capacity = _safe_env_int(
            "AGENT_TENANT_RATE_BURST_CAPACITY",
            per_tenant_limit * 30,
            minimum=1,
        )
        _AGENT_RUN_LIMITER = TenantConcurrencyLimiter(
            per_tenant_limit=per_tenant_limit,
            max_tracked_tenants=_safe_env_int("AGENT_TENANT_LIMITER_MAX_TENANTS", 2000, minimum=1),
            idle_ttl_seconds=_safe_env_float(
                "AGENT_TENANT_LIMITER_IDLE_TTL_SECONDS", 900.0, minimum=0.0
            ),
            retry_after_seconds=_safe_env_float(
                "AGENT_TENANT_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1
            ),
            refill_rate=_safe_env_float(
                "AGENT_TENANT_RATE_REFILL_PER_SECOND",
                float(per_tenant_limit * 10),
                minimum=0.0,
            ),
            burst_capacity=burst_capacity,
            warm_start_cooldown_seconds=_safe_env_float(
                "AGENT_TENANT_WARM_START_COOLDOWN_SECONDS",
                30.0,
                minimum=0.0,
            ),
            warm_start_per_tenant_limit=_safe_env_int(
                "AGENT_TENANT_WARM_START_MAX_CONCURRENT_RUNS",
                max(1, per_tenant_limit // 2),
                minimum=1,
            ),
            warm_start_burst_capacity=_safe_env_int(
                "AGENT_TENANT_WARM_START_BURST_CAPACITY",
                max(1, burst_capacity // 2),
                minimum=1,
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
        per_tenant_limit = _safe_env_int("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", 3, minimum=1)
        burst_capacity = _safe_env_int(
            "MCP_TENANT_RATE_BURST_CAPACITY",
            per_tenant_limit * 30,
            minimum=1,
        )
        _MCP_TOOL_LIMITER = TenantConcurrencyLimiter(
            per_tenant_limit=per_tenant_limit,
            max_tracked_tenants=_safe_env_int("MCP_TENANT_LIMITER_MAX_TENANTS", 2000, minimum=1),
            idle_ttl_seconds=_safe_env_float(
                "MCP_TENANT_LIMITER_IDLE_TTL_SECONDS", 900.0, minimum=0.0
            ),
            retry_after_seconds=_safe_env_float(
                "MCP_TENANT_LIMIT_RETRY_AFTER_SECONDS", 1.0, minimum=0.1
            ),
            refill_rate=_safe_env_float(
                "MCP_TENANT_RATE_REFILL_PER_SECOND",
                float(per_tenant_limit * 10),
                minimum=0.0,
            ),
            burst_capacity=burst_capacity,
            warm_start_cooldown_seconds=_safe_env_float(
                "MCP_TENANT_WARM_START_COOLDOWN_SECONDS",
                30.0,
                minimum=0.0,
            ),
            warm_start_per_tenant_limit=_safe_env_int(
                "MCP_TENANT_WARM_START_MAX_CONCURRENT_TOOL_CALLS",
                max(1, per_tenant_limit // 2),
                minimum=1,
            ),
            warm_start_burst_capacity=_safe_env_int(
                "MCP_TENANT_WARM_START_BURST_CAPACITY",
                max(1, burst_capacity // 2),
                minimum=1,
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

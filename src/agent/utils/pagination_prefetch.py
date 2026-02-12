"""Background pagination prefetch utilities."""

import asyncio
import hashlib
import json
import logging
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Optional, Set

from common.config.env import get_env_int, get_env_str

logger = logging.getLogger(__name__)

PrefetchFetchFn = Callable[[], Awaitable[Optional[dict[str, Any]]]]

_PREFETCH_CACHE_MAX_ENTRIES = 128
_PREFETCH_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()

_GLOBAL_PREFETCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_PREFETCH_ACTIVE_COUNT = 0
_PREFETCH_WAITING_COUNT = 0
_PREFETCH_MAX_OBSERVED = 0
_PREFETCH_COOLDOWN_UNTIL = 0.0  # time.time()


def build_query_signature(sql_query: str) -> str:
    """Build a stable hash signature for SQL identity checks."""
    return hashlib.sha256((sql_query or "").encode("utf-8")).hexdigest()


def _safe_prefetch_int(name: str, default: int, minimum: int) -> int:
    try:
        parsed = get_env_int(name, default)
    except ValueError:
        return default
    if parsed is None:
        return default
    return max(minimum, int(parsed))


def get_prefetch_config(interactive_session: bool) -> tuple[bool, int, str]:
    """Resolve prefetch enablement and concurrency settings."""
    mode = (get_env_str("AGENT_PREFETCH_NEXT_PAGE", "on") or "on").strip().lower()
    concurrency = _safe_prefetch_int("AGENT_PREFETCH_MAX_CONCURRENCY", default=1, minimum=1)
    if mode != "on":
        return False, concurrency, "disabled"
    if not interactive_session:
        return False, concurrency, "non_interactive"
    return True, concurrency, "enabled"


def get_global_prefetch_semaphore() -> asyncio.Semaphore:
    """Get or initialize the project-wide prefetch semaphore."""
    global _GLOBAL_PREFETCH_SEMAPHORE
    if _GLOBAL_PREFETCH_SEMAPHORE is None:
        limit = _safe_prefetch_int("AGENT_PREFETCH_GLOBAL_LIMIT", default=4, minimum=1)
        _GLOBAL_PREFETCH_SEMAPHORE = asyncio.Semaphore(limit)
    return _GLOBAL_PREFETCH_SEMAPHORE


def build_prefetch_cache_key(
    *,
    sql_query: str,
    tenant_id: Optional[int],
    page_token: str,
    page_size: Optional[int],
    schema_snapshot_id: Optional[str],
    seed: Optional[int],
    completeness_hint: Optional[str],
    scope_id: Optional[str] = None,
) -> str:
    """Build a stable cache key for prefetch payloads."""
    # Only include flags that affect tool behavior/results
    relevant_flags = {
        "fallback_mode": get_env_str("AGENT_CAPABILITY_FALLBACK_MODE"),
        "cap_mitigation": get_env_str("AGENT_PROVIDER_CAP_MITIGATION"),
    }
    query_signature = build_query_signature(sql_query)
    payload = {
        "query_signature": query_signature,
        "tenant_id": tenant_id,
        "page_token": page_token,
        "page_size": page_size,
        "schema_snapshot_id": schema_snapshot_id,
        "seed": seed,
        "completeness_hint": completeness_hint,
        "flags": relevant_flags,
        "scope": scope_id,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def pop_prefetched_page(cache_key: str) -> Optional[dict[str, Any]]:
    """Consume a prefetched page if it exists."""
    entry = _PREFETCH_CACHE.pop(cache_key, None)
    if isinstance(entry, dict) and "payload" in entry:
        payload = entry.get("payload")
        return payload if isinstance(payload, dict) else None
    return entry if isinstance(entry, dict) else None


def pop_prefetched_page_validated(
    cache_key: str,
    *,
    expected_tenant_id: Optional[int],
    expected_schema_snapshot_id: Optional[str],
    expected_query_signature: str,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """Consume a prefetched page and validate run-scoped context.

    Returns:
        (payload, discard_reason)
    """
    entry = _PREFETCH_CACHE.pop(cache_key, None)
    if entry is None:
        return None, None

    if isinstance(entry, dict) and "payload" in entry:
        payload = entry.get("payload")
        cached_tenant_id = entry.get("tenant_id")
        cached_schema_snapshot_id = entry.get("schema_snapshot_id")
        cached_query_signature = entry.get("query_signature")
    elif isinstance(entry, dict):
        payload = entry
        cached_tenant_id = None
        cached_schema_snapshot_id = None
        cached_query_signature = None
    else:
        return None, "malformed_entry"

    if cached_tenant_id != expected_tenant_id:
        return None, "tenant_mismatch"
    if cached_schema_snapshot_id != expected_schema_snapshot_id:
        return None, "snapshot_mismatch"
    if cached_query_signature != expected_query_signature:
        return None, "query_signature_mismatch"

    return payload if isinstance(payload, dict) else None, None


def cache_prefetched_page(
    cache_key: str,
    payload: dict[str, Any],
    *,
    tenant_id: Optional[int] = None,
    schema_snapshot_id: Optional[str] = None,
    query_signature: Optional[str] = None,
) -> None:
    """Cache a prefetched page payload with bounded memory usage."""
    _PREFETCH_CACHE[cache_key] = {
        "payload": payload,
        "tenant_id": tenant_id,
        "schema_snapshot_id": schema_snapshot_id,
        "query_signature": query_signature,
    }
    _PREFETCH_CACHE.move_to_end(cache_key)
    while len(_PREFETCH_CACHE) > _PREFETCH_CACHE_MAX_ENTRIES:
        _PREFETCH_CACHE.popitem(last=False)


class PrefetchManager:
    """Context manager for structured prefetch concurrency."""

    def __init__(self, max_concurrency: int = 1):
        """Initialize the prefetch manager."""
        self.max_concurrency = max_concurrency
        self.local_semaphore = asyncio.Semaphore(max_concurrency)
        self._task_group: Optional[asyncio.TaskGroup] = None
        self._inflight_keys: Set[str] = set()

    async def __aenter__(self):
        """Enter the context manager."""
        self._task_group = asyncio.TaskGroup()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        if self._task_group:
            await self._task_group.__aexit__(exc_type, exc_val, exc_tb)
        self._inflight_keys.clear()

    def schedule(
        self,
        cache_key: str,
        fetch_fn: PrefetchFetchFn,
        *,
        cache_context: Optional[dict[str, Any]] = None,
    ) -> tuple[bool, str]:
        """Schedule a prefetch task.

        Returns:
            Tuple of (scheduled_boolean, reason_code_string)
        """
        global _PREFETCH_WAITING_COUNT
        import time

        from common.constants.reason_codes import PrefetchSuppressionReason

        if not self._task_group:
            raise RuntimeError("PrefetchManager not entered")

        # 1. Duplicate check
        if cache_key in _PREFETCH_CACHE:
            return False, PrefetchSuppressionReason.ALREADY_CACHED.value
        if cache_key in self._inflight_keys:
            return False, PrefetchSuppressionReason.DUPLICATE_INFLIGHT.value

        # 2. Storm Control (Waiters Threshold)
        global_limit = _safe_prefetch_int("AGENT_PREFETCH_GLOBAL_LIMIT", default=4, minimum=1)
        storm_threshold = _safe_prefetch_int(
            "AGENT_PREFETCH_STORM_THRESHOLD", default=global_limit * 2, minimum=1
        )
        if _PREFETCH_WAITING_COUNT >= storm_threshold:
            logger.debug(f"Prefetch storm detected ({_PREFETCH_WAITING_COUNT} waiters). Skipping.")
            return False, PrefetchSuppressionReason.STORM_WAITERS.value

        # 3. Backoff / Cooldown Check
        if time.time() < _PREFETCH_COOLDOWN_UNTIL:
            logger.debug("Prefetch in cooldown status. Skipping.")
            return False, PrefetchSuppressionReason.COOLDOWN_ACTIVE.value

        _PREFETCH_WAITING_COUNT += 1
        self._inflight_keys.add(cache_key)

        # Capture current telemetry context to propagate to background task
        from agent.telemetry import telemetry

        ctx = telemetry.capture_context()

        self._task_group.create_task(self._run_task(cache_key, fetch_fn, ctx, cache_context))
        return True, PrefetchSuppressionReason.SCHEDULED.value

    async def _run_task(
        self,
        cache_key: str,
        fetch_fn: PrefetchFetchFn,
        ctx: Any,
        cache_context: Optional[dict[str, Any]],
    ):
        global _PREFETCH_ACTIVE_COUNT, _PREFETCH_MAX_OBSERVED
        global _PREFETCH_WAITING_COUNT, _PREFETCH_COOLDOWN_UNTIL
        import time

        payload = None
        global_semaphore = get_global_prefetch_semaphore()

        # Robust counter management
        # Waiting count was incremented in schedule()
        waiting_decremented = False
        active_incremented = False

        from agent.telemetry import telemetry

        # Restore telemetry context so child spans/logs are correlated
        with telemetry.use_context(ctx):
            try:
                async with self.local_semaphore:
                    # We are waiting for execution slot.
                    # Once we acquire local semaphore, we try to acquire global.
                    try:
                        async with global_semaphore:
                            # Once we have the global semaphore, we are no longer waiting/pending
                            if not waiting_decremented:
                                _PREFETCH_WAITING_COUNT = max(0, _PREFETCH_WAITING_COUNT - 1)
                                waiting_decremented = True

                            _PREFETCH_ACTIVE_COUNT += 1
                            active_incremented = True
                            _PREFETCH_MAX_OBSERVED = max(
                                _PREFETCH_MAX_OBSERVED, _PREFETCH_ACTIVE_COUNT
                            )

                            payload = await fetch_fn()
                    finally:
                        # Ensure active count is ALWAYS decremented if it was incremented
                        if active_incremented:
                            _PREFETCH_ACTIVE_COUNT = max(0, _PREFETCH_ACTIVE_COUNT - 1)

                if payload is not None:
                    context = cache_context or {}
                    cache_prefetched_page(
                        cache_key,
                        payload,
                        tenant_id=context.get("tenant_id"),
                        schema_snapshot_id=context.get("schema_snapshot_id"),
                        query_signature=context.get("query_signature"),
                    )

            except Exception as e:
                logger.debug(f"Prefetch failed for key {cache_key[:8]}: {e}")
                cooldown_seconds = _safe_prefetch_int(
                    "AGENT_PREFETCH_COOLDOWN_SECONDS", default=30, minimum=1
                )
                _PREFETCH_COOLDOWN_UNTIL = time.time() + cooldown_seconds

            finally:
                # (e.g. cancelled while waiting for local/global semaphore,
                # or exception before acquire)
                if not waiting_decremented:
                    _PREFETCH_WAITING_COUNT = max(0, _PREFETCH_WAITING_COUNT - 1)

                self._inflight_keys.discard(cache_key)


def start_prefetch_task(cache_key: str, fetch_fn: PrefetchFetchFn, max_concurrency: int) -> bool:
    """Start a background prefetch task (Deprecated)."""
    logger.warning("Called deprecated start_prefetch_task. Prefetch ignored.")
    return False


def wait_for_prefetch_tasks() -> None:
    """No-op: PrefetchManager awaits tasks on exit."""
    pass


def prefetch_diagnostics() -> dict[str, Any]:
    """Expose lightweight diagnostic counters for tests."""
    import time

    return {
        "cache_entries": len(_PREFETCH_CACHE),
        "inflight_entries": 0,
        "active_count": _PREFETCH_ACTIVE_COUNT,
        "waiting_count": _PREFETCH_WAITING_COUNT,
        "max_observed_concurrency": _PREFETCH_MAX_OBSERVED,
        "in_cooldown": time.time() < _PREFETCH_COOLDOWN_UNTIL,
    }


def reset_prefetch_state() -> None:
    """Reset in-memory prefetch state (test utility)."""
    global _PREFETCH_ACTIVE_COUNT, _PREFETCH_MAX_OBSERVED, _GLOBAL_PREFETCH_SEMAPHORE
    global _PREFETCH_WAITING_COUNT, _PREFETCH_COOLDOWN_UNTIL
    _PREFETCH_CACHE.clear()
    _PREFETCH_ACTIVE_COUNT = 0
    _PREFETCH_WAITING_COUNT = 0
    _PREFETCH_MAX_OBSERVED = 0
    _PREFETCH_COOLDOWN_UNTIL = 0.0
    _GLOBAL_PREFETCH_SEMAPHORE = None

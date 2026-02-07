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
_PREFETCH_SEMAPHORE: Optional[asyncio.Semaphore] = None
_PREFETCH_SEMAPHORE_LIMIT = 1
_PREFETCH_ACTIVE_COUNT = 0
_PREFETCH_MAX_OBSERVED = 0


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
    mode = (get_env_str("AGENT_PREFETCH_NEXT_PAGE", "off") or "off").strip().lower()
    concurrency = _safe_prefetch_int("AGENT_PREFETCH_MAX_CONCURRENCY", default=1, minimum=1)
    if mode != "on":
        return False, concurrency, "disabled"
    if not interactive_session:
        return False, concurrency, "non_interactive"
    return True, concurrency, "enabled"


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
    payload = {
        "sql_query": sql_query,
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
    return _PREFETCH_CACHE.pop(cache_key, None)


def cache_prefetched_page(cache_key: str, payload: dict[str, Any]) -> None:
    """Cache a prefetched page payload with bounded memory usage."""
    _PREFETCH_CACHE[cache_key] = payload
    _PREFETCH_CACHE.move_to_end(cache_key)
    while len(_PREFETCH_CACHE) > _PREFETCH_CACHE_MAX_ENTRIES:
        _PREFETCH_CACHE.popitem(last=False)


def _get_semaphore(limit: int) -> asyncio.Semaphore:
    global _PREFETCH_SEMAPHORE
    global _PREFETCH_SEMAPHORE_LIMIT
    if _PREFETCH_SEMAPHORE is None or _PREFETCH_SEMAPHORE_LIMIT != limit:
        _PREFETCH_SEMAPHORE = asyncio.Semaphore(limit)
        _PREFETCH_SEMAPHORE_LIMIT = limit
    return _PREFETCH_SEMAPHORE


class PrefetchManager:
    """Context manager for structured prefetch concurrency."""

    def __init__(self, max_concurrency: int = 1):
        """Initialize the prefetch manager."""
        self.max_concurrency = max_concurrency
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

    def schedule(self, cache_key: str, fetch_fn: PrefetchFetchFn) -> bool:
        """Schedule a prefetch task if not duplicate."""
        if not self._task_group:
            raise RuntimeError("PrefetchManager not entered")

        if cache_key in _PREFETCH_CACHE:
            return False
        if cache_key in self._inflight_keys:
            return False

        self._inflight_keys.add(cache_key)
        self._task_group.create_task(self._run_task(cache_key, fetch_fn))
        return True

    async def _run_task(self, cache_key: str, fetch_fn: PrefetchFetchFn):
        global _PREFETCH_ACTIVE_COUNT, _PREFETCH_MAX_OBSERVED
        semaphore = _get_semaphore(self.max_concurrency)
        try:
            async with semaphore:
                _PREFETCH_ACTIVE_COUNT += 1
                _PREFETCH_MAX_OBSERVED = max(_PREFETCH_MAX_OBSERVED, _PREFETCH_ACTIVE_COUNT)
                try:
                    payload = await fetch_fn()
                finally:
                    _PREFETCH_ACTIVE_COUNT = max(0, _PREFETCH_ACTIVE_COUNT - 1)

            if payload is not None:
                cache_prefetched_page(cache_key, payload)
        except Exception as e:
            logger.debug(f"Prefetch failed for key {cache_key[:8]}: {e}")
        finally:
            self._inflight_keys.discard(cache_key)


def start_prefetch_task(cache_key: str, fetch_fn: PrefetchFetchFn, max_concurrency: int) -> bool:
    """Start a background prefetch task (Deprecated)."""
    logger.warning("Called deprecated start_prefetch_task. Prefetch ignored.")
    return False


def wait_for_prefetch_tasks() -> None:
    """No-op: PrefetchManager awaits tasks on exit."""
    pass


def prefetch_diagnostics() -> dict[str, int]:
    """Expose lightweight diagnostic counters for tests."""
    return {
        "cache_entries": len(_PREFETCH_CACHE),
        "inflight_entries": 0,  # Not tracking global inflight anymore
        "active_count": _PREFETCH_ACTIVE_COUNT,
        "max_observed_concurrency": _PREFETCH_MAX_OBSERVED,
    }


def reset_prefetch_state() -> None:
    """Reset in-memory prefetch state (test utility)."""
    global _PREFETCH_ACTIVE_COUNT, _PREFETCH_MAX_OBSERVED
    _PREFETCH_CACHE.clear()
    _PREFETCH_ACTIVE_COUNT = 0
    _PREFETCH_MAX_OBSERVED = 0

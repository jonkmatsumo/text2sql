"""Background pagination prefetch utilities."""

import asyncio
import hashlib
import json
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Optional

from common.config.env import get_env_int, get_env_str

PrefetchFetchFn = Callable[[], Awaitable[Optional[dict[str, Any]]]]

_PREFETCH_CACHE_MAX_ENTRIES = 128
_PREFETCH_CACHE: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
_PREFETCH_INFLIGHT: set[str] = set()
_PREFETCH_TASKS: set[asyncio.Task] = set()
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


async def _run_prefetch(cache_key: str, fetch_fn: PrefetchFetchFn, max_concurrency: int) -> None:
    global _PREFETCH_ACTIVE_COUNT
    global _PREFETCH_MAX_OBSERVED
    semaphore = _get_semaphore(max_concurrency)
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
    finally:
        _PREFETCH_INFLIGHT.discard(cache_key)


def start_prefetch_task(cache_key: str, fetch_fn: PrefetchFetchFn, max_concurrency: int) -> bool:
    """Start a background prefetch task if key is not already cached/in-flight."""
    if cache_key in _PREFETCH_CACHE:
        return False
    if cache_key in _PREFETCH_INFLIGHT:
        return False

    _PREFETCH_INFLIGHT.add(cache_key)
    task = asyncio.create_task(_run_prefetch(cache_key, fetch_fn, max_concurrency))
    _PREFETCH_TASKS.add(task)

    def _cleanup(done: asyncio.Task) -> None:
        _PREFETCH_TASKS.discard(done)
        try:
            done.result()
        except Exception:
            # Prefetch failures are best-effort and should not break the agent path.
            pass

    task.add_done_callback(_cleanup)
    return True


async def wait_for_prefetch_tasks() -> None:
    """Wait for active background prefetch tasks (test utility)."""
    if not _PREFETCH_TASKS:
        return
    await asyncio.gather(*list(_PREFETCH_TASKS), return_exceptions=True)


def prefetch_diagnostics() -> dict[str, int]:
    """Expose lightweight diagnostic counters for tests."""
    return {
        "cache_entries": len(_PREFETCH_CACHE),
        "inflight_entries": len(_PREFETCH_INFLIGHT),
        "active_count": _PREFETCH_ACTIVE_COUNT,
        "max_observed_concurrency": _PREFETCH_MAX_OBSERVED,
    }


def reset_prefetch_state() -> None:
    """Reset in-memory prefetch state (test utility)."""
    global _PREFETCH_ACTIVE_COUNT
    global _PREFETCH_MAX_OBSERVED
    _PREFETCH_CACHE.clear()
    _PREFETCH_INFLIGHT.clear()
    _PREFETCH_TASKS.clear()
    _PREFETCH_ACTIVE_COUNT = 0
    _PREFETCH_MAX_OBSERVED = 0

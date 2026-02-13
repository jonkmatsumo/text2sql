"""Pluggable schema snapshot cache backend implementation."""

import abc
import asyncio
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple

from opentelemetry import trace

from common.observability.metrics import agent_metrics
from common.observability.monitor import agent_monitor


class SchemaSnapshotCache(abc.ABC):
    """Abstract base class for schema snapshot caching."""

    @abc.abstractmethod
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a schema snapshot by key."""
        pass

    @abc.abstractmethod
    async def set(self, key: str, value: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        """Store a schema snapshot with TTL."""
        pass


class MemorySchemaSnapshotCache(SchemaSnapshotCache):
    """In-memory implementation of schema snapshot cache."""

    def __init__(self):
        """Initialize memory cache."""
        self._cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a schema snapshot by key."""
        if key not in self._cache:
            return None
        value, expiry = self._cache[key]
        if time.monotonic() > expiry:
            del self._cache[key]
            return None
        return value

    async def set(self, key: str, value: Dict[str, Any], ttl_seconds: int = 3600) -> None:
        """Store a schema snapshot with TTL."""
        expiry = time.monotonic() + ttl_seconds
        self._cache[key] = (value, expiry)


_CACHE_BACKEND: Optional[SchemaSnapshotCache] = None
_SCHEMA_SNAPSHOT_ID_CACHE: "OrderedDict[int, Dict[str, Any]]" = OrderedDict()
_SCHEMA_REFRESH_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}
_SCHEMA_REFRESH_COLLISIONS: int = 0
_LAST_SCHEMA_REFRESH_TIMESTAMP: Optional[float] = None
_LAST_TENANT_REFRESH_TS: Dict[int, float] = {}
_STATE_LOCK = threading.Lock()


class SchemaRefreshLimitExceeded(RuntimeError):
    """Raised when schema refresh calls are throttled by cooldown policy."""

    def __init__(self, retry_after_seconds: float):
        """Initialize limit-exceeded error with retry delay hint."""
        self.retry_after_seconds = retry_after_seconds
        super().__init__("Schema refresh cooldown is active.")


def _tenant_cache_key(tenant_id: Optional[int]) -> int:
    return int(tenant_id or 0)


def _lock_key(tenant_id: Optional[int]) -> Tuple[int, int]:
    loop = asyncio.get_running_loop()
    return (_tenant_cache_key(tenant_id), id(loop))


def _get_refresh_lock(tenant_id: Optional[int]) -> asyncio.Lock:
    key = _lock_key(tenant_id)
    with _STATE_LOCK:
        lock = _SCHEMA_REFRESH_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _SCHEMA_REFRESH_LOCKS[key] = lock
        return lock


def _record_schema_refresh_collision() -> None:
    global _SCHEMA_REFRESH_COLLISIONS
    with _STATE_LOCK:
        _SCHEMA_REFRESH_COLLISIONS += 1
    agent_metrics.add_counter(
        "agent.schema_refresh.collisions_total",
        attributes={"scope": "schema_snapshot_id"},
        description="Count of concurrent schema snapshot refresh collisions",
    )


def _record_schema_cache_hit() -> None:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("schema.cache.hit", True)
        span.set_attribute("schema.cache.miss", False)
    agent_metrics.add_counter(
        "schema.cache.hit",
        attributes={"scope": "schema_snapshot_id"},
        description="Schema snapshot cache hits",
    )


def _record_schema_cache_miss() -> None:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("schema.cache.hit", False)
        span.set_attribute("schema.cache.miss", True)
    agent_metrics.add_counter(
        "schema.cache.miss",
        attributes={"scope": "schema_snapshot_id"},
        description="Schema snapshot cache misses",
    )


def _record_schema_refresh_count() -> None:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("schema.refresh.count", 1)
    agent_metrics.add_counter(
        "schema.refresh.count",
        attributes={"scope": "schema_snapshot_id"},
        description="Schema snapshot refresh attempts",
    )


def _record_schema_snapshot_churn() -> None:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("schema.snapshot.churn", 1)
    agent_metrics.add_counter(
        "schema.snapshot.churn",
        attributes={"scope": "schema_snapshot_id"},
        description="Count of schema snapshot id transitions",
    )


def _record_schema_refresh_cooldown_active(retry_after_seconds: float) -> None:
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute("schema.refresh.cooldown_active", True)
        span.set_attribute("retry.retry_after_seconds", float(retry_after_seconds))
    agent_monitor.increment("schema_refresh_storm")
    agent_metrics.add_counter(
        "schema.refresh.cooldown_block.count",
        attributes={"scope": "schema_snapshot_id"},
        description="Schema refresh cooldown rejections",
    )


def get_schema_refresh_collision_count() -> int:
    """Return the cumulative number of concurrent snapshot refresh collisions."""
    with _STATE_LOCK:
        return int(_SCHEMA_REFRESH_COLLISIONS)


def get_schema_snapshot_cache_size() -> int:
    """Return number of active tenant snapshot entries currently cached."""
    with _STATE_LOCK:
        return len(_SCHEMA_SNAPSHOT_ID_CACHE)


def get_last_schema_refresh_timestamp() -> Optional[float]:
    """Return unix timestamp for last successful snapshot refresh write."""
    with _STATE_LOCK:
        return _LAST_SCHEMA_REFRESH_TIMESTAMP


def get_schema_cache_ttl_seconds() -> int:
    """Return schema snapshot cache TTL in seconds from env.

    Uses SCHEMA_CACHE_TTL_SECONDS with a default of 3600 and lower bound of 1.
    """
    from common.config.env import get_env_int

    default_ttl = 3600
    try:
        raw_ttl = get_env_int("SCHEMA_CACHE_TTL_SECONDS", default_ttl)
    except ValueError:
        return default_ttl
    if raw_ttl is None:
        return default_ttl
    return max(1, int(raw_ttl))


def get_schema_cache_max_entries() -> int:
    """Return schema snapshot LRU capacity from env."""
    from common.config.env import get_env_int

    default_max_entries = 1000
    try:
        raw_max = get_env_int("SCHEMA_CACHE_MAX_ENTRIES", default_max_entries)
    except ValueError:
        return default_max_entries
    if raw_max is None:
        return default_max_entries
    return max(1, int(raw_max))


def get_schema_refresh_cooldown_seconds() -> float:
    """Return minimum interval between tenant refresh attempts."""
    from common.config.env import get_env_float

    default_cooldown = 0.0
    try:
        raw_cooldown = get_env_float("SCHEMA_REFRESH_COOLDOWN_SECONDS", default_cooldown)
    except ValueError:
        return default_cooldown
    if raw_cooldown is None:
        return default_cooldown
    return max(0.0, float(raw_cooldown))


def _evict_snapshot_lru_if_needed() -> None:
    max_entries = get_schema_cache_max_entries()
    while len(_SCHEMA_SNAPSHOT_ID_CACHE) > max_entries:
        _SCHEMA_SNAPSHOT_ID_CACHE.popitem(last=False)


def get_cached_schema_snapshot_id(
    tenant_id: Optional[int],
    *,
    now: Optional[float] = None,
) -> Optional[str]:
    """Get cached schema snapshot id for tenant when TTL is still valid."""
    cache_key = _tenant_cache_key(tenant_id)
    with _STATE_LOCK:
        entry = _SCHEMA_SNAPSHOT_ID_CACHE.get(cache_key)
    if not entry:
        _record_schema_cache_miss()
        return None

    cached_at = entry.get("cached_at")
    if not isinstance(cached_at, (int, float)):
        with _STATE_LOCK:
            _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        return None

    now_value = now if now is not None else time.monotonic()
    if now_value - float(cached_at) >= get_schema_cache_ttl_seconds():
        with _STATE_LOCK:
            _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        _record_schema_cache_miss()
        return None

    snapshot_id = entry.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        with _STATE_LOCK:
            _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        _record_schema_cache_miss()
        return None
    with _STATE_LOCK:
        if cache_key in _SCHEMA_SNAPSHOT_ID_CACHE:
            _SCHEMA_SNAPSHOT_ID_CACHE.move_to_end(cache_key)
    _record_schema_cache_hit()
    return snapshot_id


def set_cached_schema_snapshot_id(
    tenant_id: Optional[int],
    snapshot_id: str,
    *,
    now: Optional[float] = None,
) -> None:
    """Store schema snapshot id with timestamp for TTL expiry checks.

    Stale writes (older timestamps than the current cached entry) are ignored
    to reduce race-condition regressions between refresh and execute flows.
    """
    if not isinstance(snapshot_id, str) or not snapshot_id:
        return
    global _LAST_SCHEMA_REFRESH_TIMESTAMP
    cache_key = _tenant_cache_key(tenant_id)
    cached_at = now if now is not None else time.monotonic()

    with _STATE_LOCK:
        existing_entry = _SCHEMA_SNAPSHOT_ID_CACHE.get(cache_key)
    if existing_entry:
        existing_cached_at = existing_entry.get("cached_at")
        if isinstance(existing_cached_at, (int, float)) and float(cached_at) < float(
            existing_cached_at
        ):
            return

    with _STATE_LOCK:
        previous_snapshot_id = None
        if existing_entry and isinstance(existing_entry.get("snapshot_id"), str):
            previous_snapshot_id = existing_entry.get("snapshot_id")
        _SCHEMA_SNAPSHOT_ID_CACHE[cache_key] = {
            "snapshot_id": snapshot_id,
            "cached_at": cached_at,
        }
        _SCHEMA_SNAPSHOT_ID_CACHE.move_to_end(cache_key)
        _evict_snapshot_lru_if_needed()
        _LAST_SCHEMA_REFRESH_TIMESTAMP = time.time()
        _LAST_TENANT_REFRESH_TS[cache_key] = float(cached_at)
    if previous_snapshot_id and previous_snapshot_id != snapshot_id:
        _record_schema_snapshot_churn()


async def get_or_refresh_schema_snapshot_id(
    tenant_id: Optional[int],
    refresh_fn,
) -> Optional[str]:
    """Get snapshot id from cache or refresh with a per-tenant single-flight guard."""
    cached_snapshot_id = get_cached_schema_snapshot_id(tenant_id)
    if cached_snapshot_id:
        return cached_snapshot_id

    refresh_lock = _get_refresh_lock(tenant_id)
    if refresh_lock.locked():
        _record_schema_refresh_collision()

    async with refresh_lock:
        cached_snapshot_id = get_cached_schema_snapshot_id(tenant_id)
        if cached_snapshot_id:
            return cached_snapshot_id

        cooldown_seconds = get_schema_refresh_cooldown_seconds()
        cache_key = _tenant_cache_key(tenant_id)
        now = time.monotonic()
        if cooldown_seconds > 0:
            with _STATE_LOCK:
                last_refresh_ts = _LAST_TENANT_REFRESH_TS.get(cache_key)
            if isinstance(last_refresh_ts, (int, float)):
                elapsed = now - float(last_refresh_ts)
                if elapsed < cooldown_seconds:
                    retry_after_seconds = max(0.1, float(cooldown_seconds - elapsed))
                    _record_schema_refresh_cooldown_active(retry_after_seconds)
                    raise SchemaRefreshLimitExceeded(retry_after_seconds)

        _record_schema_refresh_count()
        snapshot_id = await refresh_fn()
        with _STATE_LOCK:
            _LAST_TENANT_REFRESH_TS[cache_key] = now
        if isinstance(snapshot_id, str) and snapshot_id and snapshot_id != "unknown":
            set_cached_schema_snapshot_id(tenant_id, snapshot_id)
        return snapshot_id


def get_schema_cache() -> SchemaSnapshotCache:
    """Get the configured schema cache backend."""
    global _CACHE_BACKEND
    if _CACHE_BACKEND is None:
        # Default to memory for now.
        # Future: Check env var AGENT_SCHEMA_SNAPSHOT_CACHE_BACKEND (redis, etc)
        _CACHE_BACKEND = MemorySchemaSnapshotCache()
    return _CACHE_BACKEND


def reset_schema_cache() -> None:
    """Reset the pluggable schema cache backend (test utility)."""
    global _CACHE_BACKEND
    global _SCHEMA_REFRESH_COLLISIONS
    global _LAST_SCHEMA_REFRESH_TIMESTAMP
    _CACHE_BACKEND = None
    with _STATE_LOCK:
        _SCHEMA_SNAPSHOT_ID_CACHE.clear()
        _SCHEMA_REFRESH_LOCKS.clear()
        _LAST_TENANT_REFRESH_TS.clear()
        _SCHEMA_REFRESH_COLLISIONS = 0
        _LAST_SCHEMA_REFRESH_TIMESTAMP = None

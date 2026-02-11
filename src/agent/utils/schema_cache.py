"""Pluggable schema snapshot cache backend implementation."""

import abc
import asyncio
import threading
import time
from typing import Any, Dict, Optional, Tuple

from common.observability.metrics import agent_metrics


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
_SCHEMA_SNAPSHOT_ID_CACHE: Dict[int, Dict[str, Any]] = {}
_SCHEMA_REFRESH_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}
_SCHEMA_REFRESH_COLLISIONS: int = 0
_STATE_LOCK = threading.Lock()


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


def get_schema_refresh_collision_count() -> int:
    """Return the cumulative number of concurrent snapshot refresh collisions."""
    with _STATE_LOCK:
        return int(_SCHEMA_REFRESH_COLLISIONS)


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


def get_cached_schema_snapshot_id(
    tenant_id: Optional[int],
    *,
    now: Optional[float] = None,
) -> Optional[str]:
    """Get cached schema snapshot id for tenant when TTL is still valid."""
    cache_key = _tenant_cache_key(tenant_id)
    entry = _SCHEMA_SNAPSHOT_ID_CACHE.get(cache_key)
    if not entry:
        return None

    cached_at = entry.get("cached_at")
    if not isinstance(cached_at, (int, float)):
        _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        return None

    now_value = now if now is not None else time.monotonic()
    if now_value - float(cached_at) >= get_schema_cache_ttl_seconds():
        _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        return None

    snapshot_id = entry.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        _SCHEMA_SNAPSHOT_ID_CACHE.pop(cache_key, None)
        return None
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
    cache_key = _tenant_cache_key(tenant_id)
    cached_at = now if now is not None else time.monotonic()

    existing_entry = _SCHEMA_SNAPSHOT_ID_CACHE.get(cache_key)
    if existing_entry:
        existing_cached_at = existing_entry.get("cached_at")
        if isinstance(existing_cached_at, (int, float)) and float(cached_at) < float(
            existing_cached_at
        ):
            return

    _SCHEMA_SNAPSHOT_ID_CACHE[cache_key] = {
        "snapshot_id": snapshot_id,
        "cached_at": cached_at,
    }


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

        snapshot_id = await refresh_fn()
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
    _CACHE_BACKEND = None
    with _STATE_LOCK:
        _SCHEMA_SNAPSHOT_ID_CACHE.clear()
        _SCHEMA_REFRESH_LOCKS.clear()
        _SCHEMA_REFRESH_COLLISIONS = 0

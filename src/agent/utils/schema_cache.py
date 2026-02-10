"""Pluggable schema snapshot cache backend implementation."""

import abc
import time
from typing import Any, Dict, Optional, Tuple


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
    cache_key = int(tenant_id or 0)
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
    """Store schema snapshot id with timestamp for TTL expiry checks."""
    if not isinstance(snapshot_id, str) or not snapshot_id:
        return
    cache_key = int(tenant_id or 0)
    _SCHEMA_SNAPSHOT_ID_CACHE[cache_key] = {
        "snapshot_id": snapshot_id,
        "cached_at": now if now is not None else time.monotonic(),
    }


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
    _CACHE_BACKEND = None
    _SCHEMA_SNAPSHOT_ID_CACHE.clear()

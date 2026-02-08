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

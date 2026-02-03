import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from common.config.env import get_env_int
from common.interfaces.schema_introspector import SchemaIntrospector


@dataclass
class CacheEntry:
    """Cache entry with value and expiry time."""

    value: Any
    expires_at: float


class SchemaCache:
    """In-memory read-through cache for schema introspection."""

    def __init__(self, ttl_seconds: int = 300, max_entries: Optional[int] = None) -> None:
        """Initialize cache with TTL and optional size limits."""
        self._ttl_seconds = ttl_seconds
        if max_entries is None:
            max_entries = get_env_int("DAL_SCHEMA_CACHE_MAX_ENTRIES", 1000)
        self._max_entries = max_entries if max_entries and max_entries > 0 else 0
        self._cache: "OrderedDict[Tuple[str, str, str, Optional[str], str], CacheEntry]" = (
            OrderedDict()
        )

    def get(self, key: Tuple[str, str, str, Optional[str], str]) -> Optional[Any]:
        """Fetch a cached entry if it is still valid."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        if time.time() >= entry.expires_at:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return entry.value

    def set(self, key: Tuple[str, str, str, Optional[str], str], value: Any) -> None:
        """Store a cached entry with TTL."""
        self._prune_expired()
        if key in self._cache:
            self._cache.pop(key, None)
        self._cache[key] = CacheEntry(value=value, expires_at=time.time() + self._ttl_seconds)
        self._cache.move_to_end(key)
        self._evict_if_needed()

    def clear_all(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def clear_provider(self, provider: str) -> None:
        """Clear cached entries for a provider."""
        self._cache = OrderedDict((k, v) for k, v in self._cache.items() if k[0] != provider)

    def clear_schema(self, provider: str, schema: str) -> None:
        """Clear cached entries for a provider+schema."""
        self._cache = OrderedDict(
            (k, v) for k, v in self._cache.items() if (k[0], k[2]) != (provider, schema)
        )

    def clear_table(self, provider: str, schema: str, table: str) -> None:
        """Clear cached entries for a provider+schema+table."""
        self._cache = OrderedDict(
            (k, v)
            for k, v in self._cache.items()
            if (k[0], k[2], k[3]) != (provider, schema, table)
        )

    def invalidate(
        self,
        provider: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """Invalidate cached entries based on scope."""
        if provider is None:
            self.clear_all()
            return
        if schema is None:
            self.clear_provider(provider)
            return
        if table is None:
            self.clear_schema(provider, schema)
            return
        self.clear_table(provider, schema, table)

    def _prune_expired(self) -> None:
        now = time.time()
        expired = [key for key, entry in self._cache.items() if now >= entry.expires_at]
        for key in expired:
            self._cache.pop(key, None)

    def _evict_if_needed(self) -> None:
        if self._max_entries <= 0:
            return
        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)


class CachedSchemaIntrospector(SchemaIntrospector):
    """SchemaIntrospector wrapper that uses a read-through cache."""

    def __init__(self, provider: str, wrapped: SchemaIntrospector, cache: SchemaCache) -> None:
        """Wrap a SchemaIntrospector with cache support."""
        self._provider = provider
        self._wrapped = wrapped
        self._cache = cache
        self._logger = logging.getLogger(__name__)

    async def list_table_names(self, schema: str = "public"):
        """List table names with cache support."""
        key = (self._provider, "schema", schema, None, "list_table_names")
        cached = self._cache.get(key)
        if cached is not None:
            self._logger.info(
                "schema_cache_hit provider=%s schema=%s method=list_table_names",
                self._provider,
                schema,
            )
            return cached
        result = await self._wrapped.list_table_names(schema=schema)
        self._cache.set(key, result)
        return result

    async def get_table_def(self, table_name: str, schema: str = "public"):
        """Get table definitions with cache support."""
        key = (self._provider, "schema", schema, table_name, "get_table_def")
        cached = self._cache.get(key)
        if cached is not None:
            self._logger.info(
                "schema_cache_hit provider=%s schema=%s table=%s method=get_table_def",
                self._provider,
                schema,
                table_name,
            )
            return cached
        result = await self._wrapped.get_table_def(table_name=table_name, schema=schema)
        self._cache.set(key, result)
        return result

    async def get_sample_rows(self, table_name: str, limit: int = 3, schema: str = "public"):
        """Get sample rows with cache support."""
        key = (self._provider, "schema", schema, table_name, f"get_sample_rows:{limit}")
        cached = self._cache.get(key)
        if cached is not None:
            self._logger.info(
                "schema_cache_hit provider=%s schema=%s table=%s method=get_sample_rows",
                self._provider,
                schema,
                table_name,
            )
            return cached
        result = await self._wrapped.get_sample_rows(
            table_name=table_name, limit=limit, schema=schema
        )
        self._cache.set(key, result)
        return result


SCHEMA_CACHE = SchemaCache()

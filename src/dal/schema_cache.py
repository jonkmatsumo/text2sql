import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional, Protocol, Tuple, runtime_checkable

from opentelemetry import trace

from common.config.env import get_env_int
from common.interfaces.schema_introspector import SchemaIntrospector


@dataclass
class CacheEntry:
    """Cache entry with value and expiry time."""

    value: Any
    expires_at: float


@runtime_checkable
class SchemaCacheBackend(Protocol):
    """Protocol for schema cache storage backends (In-memory, Redis, etc.)."""

    def get(self, key: Tuple[str, str, str, Optional[str], str]) -> Optional[Any]:
        """Fetch a cached entry."""
        ...

    def set(self, key: Tuple[str, str, str, Optional[str], str], value: Any, ttl: int) -> None:
        """Store a cached entry."""
        ...

    def clear(
        self,
        provider: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> int:
        """Invalidate entries by scope and return count returned."""
        ...


class InMemorySchemaCacheBackend:
    """Default in-memory implementation of SchemaCacheBackend."""

    def __init__(self, max_entries: int = 1000) -> None:
        """Initialize with max entries."""
        self._max_entries = max_entries
        self._cache: "OrderedDict[Tuple[str, str, str, Optional[str], str], CacheEntry]" = (
            OrderedDict()
        )
        self._logger = logging.getLogger(__name__)

    def get(self, key: Tuple[str, str, str, Optional[str], str]) -> Optional[Any]:
        """Get entry from in-memory dict."""
        entry = self._cache.get(key)
        if entry is None:
            return None
        if time.time() >= entry.expires_at:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return entry.value

    def set(self, key: Tuple[str, str, str, Optional[str], str], value: Any, ttl: int) -> None:
        """Set entry in in-memory dict."""
        if key in self._cache:
            self._cache.pop(key, None)
        self._cache[key] = CacheEntry(value=value, expires_at=time.time() + ttl)
        self._cache.move_to_end(key)
        self._evict_if_needed()

    def clear(
        self,
        provider: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> int:
        """Clear entries by scope and return count."""
        if provider is None:
            count = len(self._cache)
            self._cache.clear()
            return count

        # Filter based on scope
        if schema is None:
            keys_to_remove = [k for k in self._cache.keys() if k[0] == provider]
        elif table is None:
            keys_to_remove = [k for k in self._cache.keys() if (k[0], k[2]) == (provider, schema)]
        else:
            keys_to_remove = [
                k for k in self._cache.keys() if (k[0], k[2], k[3]) == (provider, schema, table)
            ]

        for k in keys_to_remove:
            self._cache.pop(k, None)

        return len(keys_to_remove)

    def _evict_if_needed(self) -> None:
        if self._max_entries <= 0:
            return
        while len(self._cache) > self._max_entries:
            key, _ = self._cache.popitem(last=False)
            self._logger.info("schema_cache_evict provider=%s key=%s", key[0], key)

    def __len__(self) -> int:
        """Return number of entries."""
        return len(self._cache)


class SchemaCache:
    """In-memory read-through cache for schema introspection."""

    def __init__(
        self,
        ttl_seconds: Optional[int] = None,
        max_entries: Optional[int] = None,
        backend: Optional[SchemaCacheBackend] = None,
    ) -> None:
        """Initialize cache with TTL and optional size limits or custom backend."""
        if ttl_seconds is None:
            ttl_seconds = get_env_int("DAL_SCHEMA_CACHE_TTL_SECONDS", 300)
        self._default_ttl = ttl_seconds

        if max_entries is None:
            max_entries = get_env_int("DAL_SCHEMA_CACHE_MAX_ENTRIES", 1000)

        self._backend = backend or InMemorySchemaCacheBackend(max_entries=max_entries or 1000)
        self._logger = logging.getLogger(__name__)
        self._tracer = trace.get_tracer(__name__)

    def _get_ttl_for_provider(self, provider: str) -> int:
        """Get TTL for a specific provider from env or default."""
        env_key = f"DAL_SCHEMA_CACHE_TTL_{provider.upper()}"
        return get_env_int(env_key, self._default_ttl)

    def get(self, key: Tuple[str, str, str, Optional[str], str]) -> Optional[Any]:
        """Fetch a cached entry if it is still valid."""
        return self._backend.get(key)

    def set(self, key: Tuple[str, str, str, Optional[str], str], value: Any) -> None:
        """Store a cached entry with TTL."""
        ttl = self._get_ttl_for_provider(key[0])
        self._backend.set(key, value, ttl)

    def clear_all(self) -> None:
        """Clear all cached entries."""
        self.invalidate()

    def clear_provider(self, provider: str) -> None:
        """Clear cached entries for a provider."""
        self.invalidate(provider=provider)

    def clear_schema(self, provider: str, schema: str) -> None:
        """Clear cached entries for a provider+schema."""
        self.invalidate(provider=provider, schema=schema)

    def clear_table(self, provider: str, schema: str, table: str) -> None:
        """Clear cached entries for a provider+schema+table."""
        self.invalidate(provider=provider, schema=schema, table=table)

    def invalidate(
        self,
        provider: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        """Invalidate cached entries based on scope and emit telemetry."""
        scope = "global"
        if provider:
            scope = "provider"
            if schema:
                scope = "schema"
                if table:
                    scope = "table"

        with self._tracer.start_as_current_span("schema.cache.invalidate") as span:
            span.set_attribute("schema.cache.scope", scope)
            if provider:
                span.set_attribute("schema.cache.provider", provider)
            if schema:
                span.set_attribute("schema.cache.schema_name", schema)
            if table:
                span.set_attribute("schema.cache.table_name", table)

            self._logger.info(
                "schema_cache_invalidate scope=%s provider=%s schema=%s table=%s",
                scope,
                provider,
                schema,
                table,
            )
            count = self._backend.clear(provider=provider, schema=schema, table=table)
            span.set_attribute("schema.cache.entries_cleared", count)


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

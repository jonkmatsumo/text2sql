from typing import List, Optional, Protocol, runtime_checkable

from mcp_server.models import CacheLookupResult


@runtime_checkable
class CacheStore(Protocol):
    """Protocol for semantic cache backends.

    Implementations must provide:
    - lookup: Pure retrieval without side effects
    - record_hit: Fire-and-forget hit counting (eventual consistency OK)
    - store: Store a new cache entry

    The split between lookup and record_hit accommodates eventual consistency
    stores like Pinecone that lack atomic increment operations.
    """

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
        cache_type: str = "sql",
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by embedding similarity.

        This is a pure retrieval operation with no side effects.
        Use record_hit() separately to update access statistics.

        Args:
            query_embedding: The embedding vector of the query.
            tenant_id: Tenant identifier for isolation.
            threshold: Minimum similarity threshold (0.0 to 1.0).

        Returns:
            CacheLookupResult if a match is found above threshold, None otherwise.
        """
        ...

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit for statistics.

        This is a fire-and-forget operation. Implementations may use
        eventual consistency (e.g., async queue to Redis sidecar).

        Args:
            cache_id: The cache entry identifier.
            tenant_id: Tenant identifier for isolation.
        """
        ...

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
        cache_type: str = "sql",
    ) -> None:
        """Store a new cache entry.

        Args:
            user_query: The original user query text.
            generated_sql: The generated SQL to cache.
            query_embedding: The embedding vector of the query.
            tenant_id: Tenant identifier for isolation.
        """
        ...

    async def delete_entry(self, user_query: str, tenant_id: int) -> None:
        """Delete a cache entry (for cleanup/testing).

        Args:
            user_query: The user query string to match.
            tenant_id: Tenant identifier.
        """
        ...

    async def prune_legacy_entries(self) -> int:
        """Prune cache entries dependent on obsolete schema versions.

        Returns:
            Number of deleted rows.
        """
        ...

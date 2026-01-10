import logging
from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.cache_store import CacheStore
from mcp_server.dal.postgres.common import _format_vector
from mcp_server.models import CacheLookupResult

logger = logging.getLogger(__name__)


class PgSemanticCache(CacheStore):
    """PostgreSQL implementation of Semantic Cache using pgvector."""

    # Schema Versioning: Increment this when changing embedding model or retrieval algorithm.
    # Obsolete entries will be ignored by lookup() and pruned by prune_legacy_entries().
    CURRENT_SCHEMA_VERSION = "v1"

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
        cache_type: str = "sql",
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by embedding similarity."""
        pg_vector = _format_vector(query_embedding)

        # 1 - distance = similarity (cosine)
        # Excludes tombstoned entries
        query = """
            SELECT
                cache_id,
                generated_sql,
                user_query,
                (1 - (query_embedding <=> $1)) as similarity
            FROM semantic_cache
            WHERE tenant_id = $2
            AND schema_version = $4
            AND cache_type = $5
            AND query_embedding IS NOT NULL
            AND (is_tombstoned IS NULL OR is_tombstoned = FALSE)
            AND (1 - (query_embedding <=> $1)) >= $3
            ORDER BY similarity DESC
            LIMIT 1
        """

        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(
                query,
                pg_vector,
                tenant_id,
                threshold,
                self.CURRENT_SCHEMA_VERSION,
                cache_type,
            )

        if row:
            return CacheLookupResult(
                cache_id=str(row["cache_id"]),
                value=row["generated_sql"],
                similarity=row["similarity"],
                metadata={"user_query": row["user_query"]},
            )
        return None

    async def lookup_by_fingerprint(
        self,
        fingerprint_key: str,
        tenant_id: int,
        cache_type: str = "sql",
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by exact fingerprint key (O(1)).

        Args:
            fingerprint_key: SHA256 hash of the semantic fingerprint.
            tenant_id: Tenant identifier for isolation.
            cache_type: Type of cache entry.

        Returns:
            CacheLookupResult if exact match found, None otherwise.
        """
        query = """
            SELECT cache_id, generated_sql, user_query
            FROM semantic_cache
            WHERE tenant_id = $1
            AND signature_key = $2
            AND cache_type = $3
            AND (is_tombstoned IS NULL OR is_tombstoned = FALSE)
            LIMIT 1
        """

        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, tenant_id, fingerprint_key, cache_type)

        if row:
            return CacheLookupResult(
                cache_id=str(row["cache_id"]),
                value=row["generated_sql"],
                similarity=1.0,  # Exact match
                metadata={"user_query": row["user_query"], "match_type": "fingerprint"},
            )
        return None

    async def lookup_candidates(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.90,
        limit: int = 3,
        cache_type: str = "sql",
    ) -> List[CacheLookupResult]:
        """Lookup multiple cache candidates for margin checking.

        Returns top N candidates above threshold, sorted by similarity descending.
        Used for detecting dense clusters where semantic similarity is unreliable.

        Args:
            query_embedding: The embedding vector of the query.
            tenant_id: Tenant identifier for isolation.
            threshold: Minimum similarity threshold (0.0 to 1.0).
            limit: Maximum number of candidates to return.
            cache_type: Type of cache entry.

        Returns:
            List of CacheLookupResult sorted by similarity (highest first).
        """
        pg_vector = _format_vector(query_embedding)

        query = """
            SELECT
                cache_id,
                generated_sql,
                user_query,
                (1 - (query_embedding <=> $1)) as similarity
            FROM semantic_cache
            WHERE tenant_id = $2
            AND schema_version = $5
            AND cache_type = $6
            AND query_embedding IS NOT NULL
            AND (is_tombstoned IS NULL OR is_tombstoned = FALSE)
            AND (1 - (query_embedding <=> $1)) >= $3
            ORDER BY similarity DESC
            LIMIT $4
        """

        async with Database.get_connection(tenant_id) as conn:
            rows = await conn.fetch(
                query,
                pg_vector,
                tenant_id,
                threshold,
                limit,
                self.CURRENT_SCHEMA_VERSION,
                cache_type,
            )

        return [
            CacheLookupResult(
                cache_id=str(row["cache_id"]),
                value=row["generated_sql"],
                similarity=row["similarity"],
                metadata={"user_query": row["user_query"]},
            )
            for row in rows
        ]

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit (fire-and-forget)."""
        try:
            # cache_id stored as int in Postgres
            id_val = int(cache_id)
        except ValueError:
            # Handle string IDs if we ever move to UUIDs, but for now schema is int
            return

        query = """
            UPDATE semantic_cache
            SET hit_count = hit_count + 1,
                last_accessed_at = NOW()
            WHERE cache_id = $1 AND tenant_id = $2
        """
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(query, id_val, tenant_id)

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
        cache_type: str = "sql",
    ) -> None:
        """Store a new cache entry."""
        pg_vector = _format_vector(query_embedding)

        query = """
            INSERT INTO semantic_cache (
                tenant_id, user_query, query_embedding, generated_sql, schema_version, cache_type
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
        """
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(
                query,
                tenant_id,
                user_query,
                pg_vector,
                generated_sql,
                self.CURRENT_SCHEMA_VERSION,
                cache_type,
            )

    async def tombstone_entry(self, cache_id: str, tenant_id: int, reason: str) -> bool:
        """Mark a cache entry as tombstoned (invalid).

        Tombstoned entries are excluded from lookup() but retained for audit.

        Args:
            cache_id: The cache entry ID to tombstone.
            tenant_id: Tenant scope for security.
            reason: Reason for tombstoning (e.g., "rating_mismatch: expected PG, found G").

        Returns:
            True if entry was tombstoned, False if not found.
        """
        try:
            id_val = int(cache_id)
        except ValueError:
            logger.warning(f"Invalid cache_id for tombstone: {cache_id}")
            return False

        query = """
            UPDATE semantic_cache
            SET is_tombstoned = TRUE,
                tombstone_reason = $3,
                tombstoned_at = NOW()
            WHERE cache_id = $1 AND tenant_id = $2
        """
        async with Database.get_connection(tenant_id) as conn:
            result = await conn.execute(query, id_val, tenant_id, reason)
            # asyncpg execute returns "UPDATE <count>" string
            count = int(result.split(" ")[-1])
            if count > 0:
                logger.info(f"Tombstoned cache entry {cache_id}: {reason}")
                return True
            return False

    async def delete_entry(self, user_query: str, tenant_id: int) -> None:
        """Delete a cache entry (for cleanup/testing)."""
        query = "DELETE FROM semantic_cache WHERE user_query = $1 AND tenant_id = $2"
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(query, user_query, tenant_id)

    async def prune_legacy_entries(self) -> int:
        """Prune cache entries dependent on obsolete schema versions."""
        query = "DELETE FROM semantic_cache WHERE schema_version != $1"
        try:
            async with Database.get_connection() as conn:
                result = await conn.execute(query, self.CURRENT_SCHEMA_VERSION)
                # asyncpg execute returns "DELETE <count>" string
                count = int(result.split(" ")[-1])
                if count > 0:
                    logger.warning(
                        f"🧹 Pruned {count} obsolete cache entries "
                        f"(Version != {self.CURRENT_SCHEMA_VERSION})"
                    )
                return count
        except Exception as e:
            logger.error(f"Failed to prune legacy entries: {e}")
            return 0

    async def prune_tombstoned_entries(self, older_than_days: int = 30) -> int:
        """Prune tombstoned entries older than specified days.

        Args:
            older_than_days: Delete tombstoned entries older than this many days.

        Returns:
            Number of entries deleted.
        """
        query = """
            DELETE FROM semantic_cache
            WHERE is_tombstoned = TRUE
            AND tombstoned_at < NOW() - INTERVAL '%s days'
        """
        try:
            async with Database.get_connection() as conn:
                result = await conn.execute(query % older_than_days)
                count = int(result.split(" ")[-1])
                if count > 0:
                    logger.info(
                        f"🧹 Pruned {count} tombstoned cache entries "
                        f"(older than {older_than_days} days)"
                    )
                return count
        except Exception as e:
            logger.error(f"Failed to prune tombstoned entries: {e}")
            return 0

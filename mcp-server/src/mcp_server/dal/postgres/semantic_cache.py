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
        query = """
            SELECT
                cache_id,
                generated_sql,
                (1 - (query_embedding <=> $1)) as similarity
            FROM semantic_cache
            WHERE tenant_id = $2
            AND schema_version = $4
            AND cache_type = $5
            AND query_embedding IS NOT NULL
            AND (1 - (query_embedding <=> $1)) >= $3
            ORDER BY similarity DESC
            LIMIT 1
        """

        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(
                query, pg_vector, tenant_id, threshold, self.CURRENT_SCHEMA_VERSION
            )

        if row:
            return CacheLookupResult(
                cache_id=str(row["cache_id"]),
                value=row["generated_sql"],
                similarity=row["similarity"],
            )
        return None

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

import logging
from contextlib import asynccontextmanager
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

    @staticmethod
    @asynccontextmanager
    async def _get_connection(tenant_id: Optional[int] = None):
        """Get connection from control-plane pool if enabled, else main pool."""
        from mcp_server.config.control_plane import ControlPlaneDatabase

        if ControlPlaneDatabase.is_enabled():
            async with ControlPlaneDatabase.get_connection(tenant_id) as conn:
                yield conn
        else:
            async with Database.get_connection(tenant_id) as conn:
                yield conn

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

        async with self._get_connection(tenant_id) as conn:
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

        async with self._get_connection(tenant_id) as conn:
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

        async with self._get_connection(tenant_id) as conn:
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

    def _compute_fingerprint(self, user_query: str, tenant_id: int) -> str:
        """Compute SHA256 fingerprint for exact caching."""
        import hashlib

        payload = f"{tenant_id}:{user_query}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    async def _execute_dual_write(self, query: str, *args, tenant_id: Optional[int] = None):
        """Execute a write operation on both pools (Primary + Shadow)."""
        from mcp_server.config.control_plane import ControlPlaneDatabase

        # 1. Primary Write (Always execute on the pool used for Reads)
        async with self._get_connection(tenant_id) as conn:
            await conn.execute(query, *args)

        # 2. Shadow Write (Execute on the OTHER pool if available)
        try:
            is_iso = ControlPlaneDatabase.is_enabled()
            is_conf = ControlPlaneDatabase.is_configured()

            if is_iso:
                # Primary = Control. Shadow = Main.
                async with Database.get_connection(tenant_id) as conn:
                    await conn.execute(query, *args)
            elif is_conf:
                # Primary = Main. Shadow = Control.
                async with ControlPlaneDatabase.get_direct_connection(tenant_id) as conn:
                    await conn.execute(query, *args)

        except Exception as e:
            # Shadow write failure should NOT block the main operation
            logger.warning(f"Shadow write failed: {e}")

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit (fire-and-forget)."""
        try:
            id_val = int(cache_id)
        except ValueError:
            return

        query = """
            UPDATE semantic_cache
            SET hit_count = hit_count + 1,
                last_accessed_at = NOW()
            WHERE cache_id = $1 AND tenant_id = $2
        """
        await self._execute_dual_write(query, id_val, tenant_id, tenant_id=tenant_id)

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
        cache_type: str = "sql",
        signature_key: Optional[str] = None,
    ) -> None:
        """Store a new cache entry (Dual-Write)."""
        pg_vector = _format_vector(query_embedding)

        # Use provided key or compute fingerprint
        if not signature_key:
            signature_key = self._compute_fingerprint(user_query, tenant_id)

        query = """
            INSERT INTO semantic_cache (
                tenant_id,
                user_query,
                generated_sql,
                query_embedding,
                schema_version,
                cache_type,
                signature_key
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (signature_key)
            DO UPDATE SET
                generated_sql = EXCLUDED.generated_sql,
                query_embedding = EXCLUDED.query_embedding,
                last_accessed_at = NOW(),
                is_tombstoned = FALSE
        """
        await self._execute_dual_write(
            query,
            tenant_id,
            user_query,
            generated_sql,
            pg_vector,
            self.CURRENT_SCHEMA_VERSION,
            cache_type,
            signature_key,
            tenant_id=tenant_id,
        )

    async def tombstone_entry(self, cache_id: str, tenant_id: int, reason: str) -> bool:
        """Mark a cache entry as tombstoned (Dual-Write)."""
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
        # Note: _execute_dual_write doesn't return count, but we need it for return value.
        # We'll use the primary result for logic.

        success = False
        # Primary
        async with self._get_connection(tenant_id) as conn:
            result = await conn.execute(query, id_val, tenant_id, reason)
            if int(result.split(" ")[-1]) > 0:
                success = True

        # Shadow (Manual invocation since we need to separate logic)
        try:
            from mcp_server.config.control_plane import ControlPlaneDatabase

            is_iso = ControlPlaneDatabase.is_enabled()
            is_conf = ControlPlaneDatabase.is_configured()

            if is_iso:
                async with Database.get_connection(tenant_id) as conn:
                    await conn.execute(query, id_val, tenant_id, reason)
            elif is_conf:
                async with ControlPlaneDatabase.get_direct_connection(tenant_id) as conn:
                    await conn.execute(query, id_val, tenant_id, reason)
        except Exception as e:
            logger.warning(f"Shadow tombstone failed: {e}")

        if success:
            logger.info(f"Tombstoned cache entry {cache_id}: {reason}")

        return success

    async def delete_entry(self, user_query: str, tenant_id: int) -> None:
        """Delete a cache entry (for cleanup/testing)."""
        query = "DELETE FROM semantic_cache WHERE user_query = $1 AND tenant_id = $2"
        async with self._get_connection(tenant_id) as conn:
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

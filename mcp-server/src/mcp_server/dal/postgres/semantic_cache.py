import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.postgres.common import _format_vector
from mcp_server.models import CacheLookupResult

from common.interfaces.cache_store import CacheStore

logger = logging.getLogger(__name__)


class PgSemanticCache(CacheStore):
    """PostgreSQL implementation of Semantic Cache using the Unified Registry."""

    # Schema Versioning: Stored in metadata.schema_version
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
        """Lookup a cached result by embedding similarity (Global/Cross-Tenant)."""
        pg_vector = _format_vector(query_embedding)

        query = """
            SELECT
                signature_key as cache_id,
                sql_query as generated_sql,
                question as user_query,
                (1 - (embedding <=> $1)) as similarity
            FROM public.query_pairs
            WHERE (metadata->>'schema_version') = $3
            AND $4 = ANY(roles)
            AND embedding IS NOT NULL
            AND status != 'tombstoned'
            AND (1 - (embedding <=> $1)) >= $2
            ORDER BY similarity DESC
            LIMIT 1
        """

        async with self._get_connection(tenant_id) as conn:
            row = await conn.fetchrow(
                query,
                pg_vector,
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
        """Lookup a cached result by exact fingerprint key (Global/Cross-Tenant)."""
        query = """
            SELECT signature_key as cache_id, sql_query as generated_sql, question as user_query
            FROM public.query_pairs
            WHERE signature_key = $1
            AND $2 = ANY(roles)
            AND status != 'tombstoned'
            LIMIT 1
        """

        async with self._get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, fingerprint_key, cache_type)

        if row:
            return CacheLookupResult(
                cache_id=row["cache_id"],
                value=row["generated_sql"],
                similarity=1.0,
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
        """Lookup multiple cache candidates for margin checking."""
        pg_vector = _format_vector(query_embedding)

        query = """
            SELECT
                signature_key as cache_id,
                sql_query as generated_sql,
                question as user_query,
                (1 - (embedding <=> $1)) as similarity
            FROM public.query_pairs
            WHERE (metadata->>'schema_version') = $4
            AND $5 = ANY(roles)
            AND embedding IS NOT NULL
            AND status != 'tombstoned'
            AND (1 - (embedding <=> $1)) >= $2
            ORDER BY similarity DESC
            LIMIT $3
        """

        async with self._get_connection(tenant_id) as conn:
            rows = await conn.fetch(
                query,
                pg_vector,
                threshold,
                limit,
                self.CURRENT_SCHEMA_VERSION,
                cache_type,
            )

        return [
            CacheLookupResult(
                cache_id=row["cache_id"],
                value=row["generated_sql"],
                similarity=row["similarity"],
                metadata={"user_query": row["user_query"]},
            )
            for row in rows
        ]

    def _compute_fingerprint(self, user_query: str, tenant_id: int) -> str:
        """Compute SHA256 fingerprint for exact caching."""
        import hashlib

        payload = f"{user_query}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    async def _execute_dual_write(self, query: str, *args, tenant_id: Optional[int] = None):
        """Execute a write operation on both pools (Primary + Shadow)."""
        from mcp_server.config.control_plane import ControlPlaneDatabase

        async with self._get_connection(tenant_id) as conn:
            await conn.execute(query, *args)

        try:
            is_iso = ControlPlaneDatabase.is_enabled()
            is_conf = ControlPlaneDatabase.is_configured()
            if is_iso:
                async with Database.get_connection(tenant_id) as conn:
                    await conn.execute(query, *args)
            elif is_conf:
                async with ControlPlaneDatabase.get_direct_connection(tenant_id) as conn:
                    await conn.execute(query, *args)
        except Exception as e:
            logger.warning(f"Shadow write failed: {e}")

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit."""
        query = """
            UPDATE public.query_pairs
            SET performance = jsonb_set(
                    coalesce(performance, '{}'),
                    '{hit_count}',
                    (coalesce(performance->>'hit_count', '0')::int + 1)::text::jsonb
                ),
                updated_at = NOW()
            WHERE signature_key = $1 AND tenant_id = $2
        """
        await self._execute_dual_write(query, cache_id, tenant_id, tenant_id=tenant_id)

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
        cache_type: str = "sql",
        signature_key: Optional[str] = None,
    ) -> None:
        """Store a new cache entry."""
        pg_vector = _format_vector(query_embedding)
        if not signature_key:
            signature_key = self._compute_fingerprint(user_query, tenant_id)

        query = """
            INSERT INTO public.query_pairs (
                tenant_id,
                question,
                sql_query,
                embedding,
                metadata,
                roles,
                signature_key,
                fingerprint,
                status
            )
            VALUES (
                $1, $2, $3, $4,
                jsonb_build_object('schema_version', $5, 'cache_type', $6),
                ARRAY[$6::varchar], $7, $2, 'autogenerated'
            )
            ON CONFLICT (signature_key, tenant_id)
            DO UPDATE SET
                sql_query = EXCLUDED.sql_query,
                embedding = EXCLUDED.embedding,
                updated_at = NOW(),
                status = 'autogenerated',
                roles = ARRAY(SELECT DISTINCT unnest(query_pairs.roles || EXCLUDED.roles))
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
        """Mark a cache entry as tombstoned."""
        query = """
            UPDATE public.query_pairs
            SET status = 'tombstoned',
                metadata = metadata || jsonb_build_object(
                    'tombstone_reason', $3,
                    'tombstoned_at', NOW()
                ),
                updated_at = NOW()
            WHERE signature_key = $1 AND tenant_id = $2
        """
        success = False
        async with self._get_connection(tenant_id) as conn:
            result = await conn.execute(query, cache_id, tenant_id, reason)
            if int(result.split(" ")[-1]) > 0:
                success = True

        try:
            from mcp_server.config.control_plane import ControlPlaneDatabase

            is_iso = ControlPlaneDatabase.is_enabled()
            is_conf = ControlPlaneDatabase.is_configured()
            if is_iso:
                async with Database.get_connection(tenant_id) as conn:
                    await conn.execute(query, cache_id, tenant_id, reason)
            elif is_conf:
                async with ControlPlaneDatabase.get_direct_connection(tenant_id) as conn:
                    await conn.execute(query, cache_id, tenant_id, reason)
        except Exception as e:
            logger.warning(f"Shadow tombstone failed: {e}")

        return success

    async def delete_entry(self, user_query: str, tenant_id: int) -> None:
        """Delete a cache entry."""
        query = "DELETE FROM public.query_pairs WHERE question = $1 AND tenant_id = $2"
        async with self._get_connection(tenant_id) as conn:
            await conn.execute(query, user_query, tenant_id)

    async def prune_legacy_entries(self) -> int:
        """Prune cache entries dependent on obsolete schema versions."""
        query = "DELETE FROM public.query_pairs WHERE (metadata->>'schema_version') != $1"
        try:
            count = 0
            async with self._get_connection() as conn:
                result = await conn.execute(query, self.CURRENT_SCHEMA_VERSION)
                count = int(result.split(" ")[-1])
            await self._execute_dual_write(query, self.CURRENT_SCHEMA_VERSION)
            return count
        except Exception as e:
            logger.error(f"Failed to prune legacy entries: {e}")
            return 0

    async def prune_tombstoned_entries(self, older_than_days: int = 30) -> int:
        """Prune tombstoned entries older than specified days."""
        query = f"""
            DELETE FROM public.query_pairs
            WHERE status = 'tombstoned'
            AND (metadata->>'tombstoned_at')::timestamp < NOW() - INTERVAL '{older_than_days} days'
        """
        try:
            count = 0
            async with self._get_connection() as conn:
                result = await conn.execute(query)
                count = int(result.split(" ")[-1])
            await self._execute_dual_write(query)
            return count
        except Exception as e:
            logger.error(f"Failed to prune tombstoned entries: {e}")
            return 0

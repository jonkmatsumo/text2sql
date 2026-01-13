import json
import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.registry_store import RegistryStore
from mcp_server.dal.postgres.common import _format_vector
from mcp_server.models import QueryPair

logger = logging.getLogger(__name__)


class PostgresRegistryStore(RegistryStore):
    """PostgreSQL implementation of the Unified Registry."""

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

    async def store_pair(self, pair: QueryPair) -> None:
        """Upsert a query pair into the unified registry."""
        pg_vector = _format_vector(pair.embedding) if pair.embedding else None

        query = """
            INSERT INTO public.query_pairs (
                signature_key, tenant_id, fingerprint, question,
                sql_query, embedding, roles, status, metadata, performance
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (signature_key, tenant_id)
            DO UPDATE SET
                question = EXCLUDED.question,
                sql_query = EXCLUDED.sql_query,
                embedding = EXCLUDED.embedding,
                roles = ARRAY(SELECT DISTINCT unnest(query_pairs.roles || EXCLUDED.roles)),
                status = CASE
                    WHEN EXCLUDED.status = 'verified' THEN 'verified'
                    ELSE query_pairs.status
                END,
                metadata = query_pairs.metadata || EXCLUDED.metadata,
                performance = query_pairs.performance || EXCLUDED.performance,
                updated_at = NOW()
        """

        async with self._get_connection(pair.tenant_id) as conn:
            await conn.execute(
                query,
                pair.signature_key,
                pair.tenant_id,
                pair.fingerprint,
                pair.question,
                pair.sql_query,
                pg_vector,
                pair.roles,
                pair.status,
                json.dumps(pair.metadata),
                json.dumps(pair.performance),
            )

    async def lookup_by_signature(self, signature_key: str, tenant_id: int) -> Optional[QueryPair]:
        """Fetch a specific pair by its canonical signature."""
        query = """
            SELECT signature_key, tenant_id, fingerprint, question,
                   sql_query, embedding, roles, status, metadata, performance,
                   created_at, updated_at
            FROM public.query_pairs
            WHERE signature_key = $1 AND tenant_id = $2
            LIMIT 1
        """
        async with self._get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, signature_key, tenant_id)

        if not row:
            return None

        return self._row_to_model(row)

    async def lookup_semantic_candidates(
        self,
        embedding: List[float],
        tenant_id: int,
        threshold: float = 0.90,
        limit: int = 5,
        role: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[QueryPair]:
        """Search for semantically similar pairs with optional role and status filtering."""
        pg_vector = _format_vector(embedding)

        filters = ["(1 - (embedding <=> $1)) >= $2"]
        args = [pg_vector, threshold, limit]

        if role:
            args.append(role)
            filters.append(f"${len(args)} = ANY(roles)")

        if status:
            args.append(status)
            filters.append(f"status = ${len(args)}")

        where_clause = " WHERE " + " AND ".join(filters)

        query = f"""
            SELECT signature_key, tenant_id, fingerprint, question,
                   sql_query, (embedding) as embedding, roles, status, metadata, performance,
                   created_at, updated_at,
                   (1 - (embedding <=> $1)) as similarity
            FROM public.query_pairs
            {where_clause}
            ORDER BY similarity DESC
            LIMIT $3
        """

        async with self._get_connection(tenant_id) as conn:
            rows = await conn.fetch(query, *args)

        return [self._row_to_model(row) for row in rows]

    async def fetch_by_role(
        self,
        role: str,
        status: Optional[str] = None,
        tenant_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[QueryPair]:
        """Fetch pairs by role (e.g., all 'example' pairs)."""
        filters = ["$1 = ANY(roles)"]
        args = [role]

        if status:
            args.append(status)
            filters.append(f"status = ${len(args)}")

        if tenant_id:
            args.append(tenant_id)
            filters.append(f"tenant_id = ${len(args)}")

        where_clause = " WHERE " + " AND ".join(filters)

        query = f"""
            SELECT signature_key, tenant_id, fingerprint, question,
                   sql_query, embedding, roles, status, metadata, performance,
                   created_at, updated_at
            FROM public.query_pairs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${len(args) + 1}
        """
        args.append(limit)

        async with self._get_connection(tenant_id) as conn:
            rows = await conn.fetch(query, *args)

        return [self._row_to_model(row) for row in rows]

    def _row_to_model(self, row) -> QueryPair:
        """Convert a database row to a QueryPair model."""
        embedding_val = row["embedding"]
        if embedding_val and isinstance(embedding_val, str):
            embedding = json.loads(embedding_val)
        elif embedding_val:
            embedding = list(embedding_val)
        else:
            embedding = None

        return QueryPair(
            signature_key=row["signature_key"],
            tenant_id=row["tenant_id"],
            fingerprint=row["fingerprint"],
            question=row["question"],
            sql_query=row["sql_query"],
            embedding=embedding,
            roles=row["roles"],
            status=row["status"],
            metadata=(
                json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
            ),
            performance=(
                json.loads(row["performance"])
                if isinstance(row["performance"], str)
                else row["performance"]
            ),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def tombstone_pair(self, signature_key: str, tenant_id: int, reason: str) -> bool:
        """Mark a query pair as tombstoned."""
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
        async with self._get_connection(tenant_id) as conn:
            result = await conn.execute(query, signature_key, tenant_id, reason)
            return int(result.split(" ")[-1]) > 0

    async def fetch_by_signatures(
        self, signature_keys: List[str], tenant_id: int
    ) -> List[QueryPair]:
        """Fetch multiple pairs by their signature keys."""
        if not signature_keys:
            return []

        query = """
            SELECT signature_key, tenant_id, fingerprint, question,
                   sql_query, embedding, roles, status, metadata, performance,
                   created_at, updated_at
            FROM public.query_pairs
            WHERE signature_key = ANY($1) AND tenant_id = $2
        """
        async with self._get_connection(tenant_id) as conn:
            rows = await conn.fetch(query, signature_keys, tenant_id)

        return [self._row_to_model(row) for row in rows]

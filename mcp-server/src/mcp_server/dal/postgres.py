from typing import List, Optional

from mcp_server.dal.interfaces import CacheStore, ExampleStore
from mcp_server.dal.types import CacheLookupResult, Example
from mcp_server.db import Database
from mcp_server.rag import format_vector_for_postgres


class PgSemanticCache(CacheStore):
    """PostgreSQL implementation of Semantic Cache using pgvector."""

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by embedding similarity."""
        pg_vector = format_vector_for_postgres(query_embedding)

        # 1 - distance = similarity (cosine)
        query = """
            SELECT
                cache_id,
                generated_sql,
                (1 - (query_embedding <=> $1)) as similarity
            FROM semantic_cache
            WHERE tenant_id = $2
            AND query_embedding IS NOT NULL
            AND (1 - (query_embedding <=> $1)) >= $3
            ORDER BY similarity DESC
            LIMIT 1
        """

        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, pg_vector, tenant_id, threshold)

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
    ) -> None:
        """Store a new cache entry."""
        pg_vector = format_vector_for_postgres(query_embedding)

        query = """
            INSERT INTO semantic_cache (tenant_id, user_query, query_embedding, generated_sql)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
        """
        async with Database.get_connection(tenant_id) as conn:
            await conn.execute(query, tenant_id, user_query, pg_vector, generated_sql)


class PostgresExampleStore(ExampleStore):
    """Postgres implementation of ExampleStore."""

    async def fetch_all_examples(self) -> List[Example]:
        """Fetch all examples from sql_examples table.

        Returns:
            List of canonical Example objects.
        """
        import json

        query = """
            SELECT id, question, sql_query, embedding
            FROM sql_examples
            WHERE embedding IS NOT NULL
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)

        examples = []
        for row in rows:
            embedding_val = row["embedding"]
            if isinstance(embedding_val, str):
                vector = json.loads(embedding_val)
            else:
                vector = list(embedding_val)

            examples.append(
                Example(
                    id=row["id"],
                    question=row["question"],
                    sql_query=row["sql_query"],
                    embedding=vector,
                )
            )

        return examples

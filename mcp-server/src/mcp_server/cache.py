"""Semantic caching module for SQL query results."""

from typing import Optional

from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres

# Conservative threshold to prevent serving wrong SQL for nuanced queries
# Research suggests 0.92-0.95 for BI applications where accuracy is paramount
SIMILARITY_THRESHOLD = 0.95


async def lookup_cache(user_query: str, tenant_id: int) -> Optional[str]:
    """
    Check cache for a semantically equivalent query from the SAME tenant.

    Uses vector similarity search to find cached SQL that matches the user's intent.
    Only returns cached SQL if similarity exceeds threshold (0.95).

    Args:
        user_query: The user's natural language question
        tenant_id: Tenant identifier (required for isolation)

    Returns:
        Cached SQL query string if match found, None otherwise
    """
    embedding = RagEngine.embed_text(user_query)
    pg_vector = format_vector_for_postgres(embedding)

    # 1 - distance = similarity (cosine)
    # We filter by tenant_id first, then do vector search
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
        row = await conn.fetchrow(query, pg_vector, tenant_id, SIMILARITY_THRESHOLD)

    if row:
        similarity = row["similarity"]
        print(f"✓ Cache Hit! Similarity: {similarity:.4f}, Cache ID: {row['cache_id']}")

        # Update hit count and last accessed timestamp
        await update_cache_access(row["cache_id"], tenant_id)

        return row["generated_sql"]

    return None


async def update_cache_access(cache_id: int, tenant_id: int):
    """Update cache entry access statistics."""
    async with Database.get_connection(tenant_id) as conn:
        await conn.execute(
            """
            UPDATE semantic_cache
            SET hit_count = hit_count + 1,
                last_accessed_at = NOW()
            WHERE cache_id = $1 AND tenant_id = $2
        """,
            cache_id,
            tenant_id,
        )


async def update_cache(user_query: str, sql: str, tenant_id: int):
    """
    Write a new confirmed SQL generation to the cache.

    Only caches successful SQL queries (those that executed without errors).

    Args:
        user_query: The user's natural language question
        sql: The generated SQL query that was successfully executed
        tenant_id: Tenant identifier (required for isolation)
    """
    embedding = RagEngine.embed_text(user_query)
    pg_vector = format_vector_for_postgres(embedding)

    query = """
        INSERT INTO semantic_cache (tenant_id, user_query, query_embedding, generated_sql)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    """

    async with Database.get_connection(tenant_id) as conn:
        await conn.execute(query, tenant_id, user_query, pg_vector, sql)

    print(f"✓ Cached SQL for tenant {tenant_id}")


async def get_cache_stats(tenant_id: Optional[int] = None) -> dict:
    """
    Get cache statistics for monitoring.

    Args:
        tenant_id: Optional tenant ID. If None, returns global stats.

    Returns:
        Dictionary with cache statistics
    """
    if tenant_id:
        query = """
            SELECT
                COUNT(*) as total_entries,
                SUM(hit_count) as total_hits,
                AVG(similarity_score) as avg_similarity
            FROM semantic_cache
            WHERE tenant_id = $1
        """
        async with Database.get_connection(tenant_id) as conn:
            row = await conn.fetchrow(query, tenant_id)
    else:
        query = """
            SELECT
                COUNT(*) as total_entries,
                SUM(hit_count) as total_hits,
                AVG(similarity_score) as avg_similarity
            FROM semantic_cache
        """
        async with Database.get_connection() as conn:
            row = await conn.fetchrow(query)

    return {
        "total_entries": row["total_entries"] or 0,
        "total_hits": row["total_hits"] or 0,
        "avg_similarity": float(row["avg_similarity"]) if row["avg_similarity"] else 0.0,
    }

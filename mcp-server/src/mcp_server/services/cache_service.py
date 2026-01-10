"""Semantic caching module for SQL query results."""

import asyncio
from typing import Optional

from mcp_server.config.database import Database
from mcp_server.models import CacheLookupResult
from mcp_server.rag import RagEngine

# Conservative threshold to prevent serving wrong SQL for nuanced queries
# Research suggests 0.92-0.95 for BI applications where accuracy is paramount
SIMILARITY_THRESHOLD = 0.90


async def lookup_cache(user_query: str, tenant_id: int) -> Optional[CacheLookupResult]:
    """
    Check cache for a semantically equivalent query from the SAME tenant.

    Uses vector similarity search to find cached SQL that matches the user's intent.
    Only returns cached SQL if similarity exceeds threshold (0.90).

    Args:
        user_query: The user's natural language question
        tenant_id: Tenant identifier (required for isolation)

    Returns:
        CacheLookupResult object if match found, None otherwise
    """
    embedding = RagEngine.embed_text(user_query)

    store = Database.get_cache_store()
    result = await store.lookup(
        embedding, tenant_id, threshold=SIMILARITY_THRESHOLD, cache_type="sql"
    )

    if result:
        print(f"✓ Cache Hit! Similarity: {result.similarity:.4f}, Cache ID: {result.cache_id}")

        # Update hit count and last accessed timestamp (fire-and-forget)
        # We don't await this to ensure low latency
        asyncio.create_task(update_cache_access(result.cache_id, tenant_id))

        return result

    return None


async def update_cache_access(cache_id: str, tenant_id: int):
    """Update cache entry access statistics."""
    store = Database.get_cache_store()
    await store.record_hit(cache_id, tenant_id)


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

    store = Database.get_cache_store()
    await store.store(
        user_query=user_query,
        generated_sql=sql,
        query_embedding=embedding,
        tenant_id=tenant_id,
        cache_type="sql",
    )

    print(f"✓ Cached SQL for tenant {tenant_id}")


async def get_cache_stats(tenant_id: Optional[int] = None) -> dict:
    """
    Get cache statistics for monitoring.

    Args:
        tenant_id: Optional tenant ID. If None, returns global stats.

    Returns:
        Dictionary with cache statistics
    """
    # Note: Stats retrieval is typically admin/monitoring function.
    # The current CacheStore protocol doesn't have a get_stats method.
    # We can either add it to the Protocol or let this function query DB.
    # However, strict DAL compliance suggests we should avoid direct DB access.
    # For now, to fully decouple, we return a stub.

    # If stats are needed, they should be added to the interface.
    return {"status": "Stats not implementing in DAL v1"}


async def prune_legacy_entries() -> int:
    """Prune legacy cache entries on startup."""
    store = Database.get_cache_store()
    return await store.prune_legacy_entries()

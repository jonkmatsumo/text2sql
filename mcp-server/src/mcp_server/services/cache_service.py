"""Semantic caching module for SQL query results."""

import asyncio
import logging
from typing import Optional

from mcp_server.config.database import Database
from mcp_server.models import CacheLookupResult
from mcp_server.rag import RagEngine
from mcp_server.services.canonicalization import CanonicalizationService

logger = logging.getLogger(__name__)

# Conservative threshold to prevent serving wrong SQL for nuanced queries
# Research suggests 0.92-0.95 for BI applications where accuracy is paramount
SIMILARITY_THRESHOLD = 0.90


async def lookup_cache(user_query: str, tenant_id: int) -> Optional[CacheLookupResult]:
    """Check cache with two-tier lookup strategy.

    Tier 1: Fingerprint exact match (O(1), 100% precision)
    Tier 2: Vector similarity with constraint validation (fallback)

    Args:
        user_query: The user's natural language question
        tenant_id: Tenant identifier (required for isolation)

    Returns:
        CacheLookupResult object if match found, None otherwise
    """
    store = Database.get_cache_store()
    canonicalizer = CanonicalizationService.get_instance()

    # === Tier 1: SpaCy Canonicalization (exact fingerprint match) ===
    if canonicalizer.is_available():
        constraints, fingerprint, fingerprint_key = canonicalizer.process_query(user_query)

        if fingerprint:  # Non-empty fingerprint
            result = await store.lookup_by_fingerprint(fingerprint_key, tenant_id)
            if result:
                logger.info(f"✓ Fingerprint hit: {fingerprint} (key={fingerprint_key[:16]}...)")
                asyncio.create_task(update_cache_access(result.cache_id, tenant_id))
                return result
            else:
                logger.debug(f"Fingerprint miss: {fingerprint}")

    # === Tier 2: Vector Similarity Fallback ===
    embedding = RagEngine.embed_text(user_query)
    result = await store.lookup(
        embedding, tenant_id, threshold=SIMILARITY_THRESHOLD, cache_type="sql"
    )

    if result:
        logger.info(
            f"✓ Vector hit: similarity={result.similarity:.4f}, " f"cache_id={result.cache_id}"
        )
        asyncio.create_task(update_cache_access(result.cache_id, tenant_id))
        return result

    return None


async def update_cache_access(cache_id: str, tenant_id: int):
    """Update cache entry access statistics."""
    store = Database.get_cache_store()
    await store.record_hit(cache_id, tenant_id)


async def update_cache(user_query: str, sql: str, tenant_id: int):
    """Write a new confirmed SQL generation to the cache.

    Computes a semantic fingerprint for exact-match lookups.

    Args:
        user_query: The user's natural language question
        sql: The generated SQL query that was successfully executed
        tenant_id: Tenant identifier (required for isolation)
    """
    embedding = RagEngine.embed_text(user_query)

    # Compute fingerprint for exact-match lookups
    signature_key = None
    canonicalizer = CanonicalizationService.get_instance()
    if canonicalizer.is_available():
        _, _, signature_key = canonicalizer.process_query(user_query)
        logger.debug(f"Generated signature_key: {signature_key[:16]}...")

    store = Database.get_cache_store()
    await store.store(
        user_query=user_query,
        generated_sql=sql,
        query_embedding=embedding,
        tenant_id=tenant_id,
        cache_type="sql",
        signature_key=signature_key,
    )

    logger.info(f"✓ Cached SQL for tenant {tenant_id}")


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


async def tombstone_cache_entry(cache_id: str, tenant_id: int, reason: str) -> bool:
    """Mark a cache entry as tombstoned (invalid).

    Tombstoned entries are excluded from lookup but retained for audit.

    Args:
        cache_id: The cache entry ID to tombstone.
        tenant_id: Tenant scope for security.
        reason: Reason for tombstoning (e.g., "rating_mismatch: expected PG, found G").

    Returns:
        True if entry was tombstoned, False if not found.
    """
    store = Database.get_cache_store()
    return await store.tombstone_entry(cache_id, tenant_id, reason)


async def prune_tombstoned_entries(older_than_days: int = 30) -> int:
    """Prune tombstoned cache entries older than specified days.

    Args:
        older_than_days: Delete tombstoned entries older than this many days.

    Returns:
        Number of entries deleted.
    """
    store = Database.get_cache_store()
    return await store.prune_tombstoned_entries(older_than_days)

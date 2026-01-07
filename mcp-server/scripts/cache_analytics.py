"""Script to analyze semantic cache performance."""

import asyncio

from mcp_server.cache import get_cache_stats
from mcp_server.db import Database


async def analyze_cache():
    """Print cache statistics and analytics."""
    await Database.init()

    try:
        # Global stats
        stats = await get_cache_stats()
        print("=== Global Cache Statistics ===")
        print(f"Total Entries: {stats['total_entries']}")
        print(f"Total Hits: {stats['total_hits']}")
        print(f"Average Similarity: {stats['avg_similarity']:.4f}")

        if stats["total_entries"] > 0:
            hit_rate = (stats["total_hits"] / stats["total_entries"]) * 100
            print(f"Hit Rate: {hit_rate:.2f}%")

        # Per-tenant stats
        async with Database.get_connection() as conn:
            tenant_stats = await conn.fetch(
                """
                SELECT
                    tenant_id,
                    COUNT(*) as entries,
                    SUM(hit_count) as hits,
                    AVG(similarity_score) as avg_similarity
                FROM semantic_cache
                GROUP BY tenant_id
                ORDER BY tenant_id
            """
            )

        if tenant_stats:
            print("\n=== Per-Tenant Statistics ===")
            for row in tenant_stats:
                print(f"Tenant {row['tenant_id']}:")
                print(f"  Entries: {row['entries']}")
                print(f"  Hits: {row['hits']}")
                print(f"  Avg Similarity: {row['avg_similarity']:.4f}")

        # Top cached queries
        async with Database.get_connection() as conn:
            top_queries = await conn.fetch(
                """
                SELECT
                    user_query,
                    hit_count,
                    similarity_score
                FROM semantic_cache
                ORDER BY hit_count DESC
                LIMIT 10
            """
            )

        if top_queries:
            print("\n=== Top 10 Cached Queries (by hit count) ===")
            for i, row in enumerate(top_queries, 1):
                query_preview = row["user_query"][:60]
                print(f"{i}. {query_preview}...")
                print(f"   Hits: {row['hit_count']}, Similarity: {row['similarity_score']:.4f}")

    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(analyze_cache())

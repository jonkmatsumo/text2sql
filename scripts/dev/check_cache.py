import argparse
import asyncio

from dotenv import load_dotenv

from mcp_server.config.database import Database
from mcp_server.services.cache import lookup_cache
from mcp_server.services.rag import RagEngine

# Load environment variables
load_dotenv()


async def check_cache(query: str, tenant_id: int):
    """Check cache status for a query and tenant."""
    print(f"Checking cache for Tenant ID: {tenant_id}")
    print(f"Query: {query}")
    print("-" * 50)

    # Init DB
    await Database.init()

    # 1. Check if ANY entries exist for this Tenant
    async with Database.get_connection(tenant_id) as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM semantic_cache WHERE tenant_id = $1 AND cache_type='sql'",
            tenant_id,
        )
        print(f"Total Cached SQL Entries for Tenant {tenant_id}: {count}")

        if count > 0:
            # List them
            rows = await conn.fetch(
                "SELECT cache_id, SUBSTRING(user_query, 1, 50) as q, generated_sql "
                "FROM semantic_cache WHERE tenant_id = $1 AND cache_type='sql'",
                tenant_id,
            )
            print("\nExisting Entries:")
            for r in rows:
                print(f" - ID {r['cache_id']}: {r['q']}...")

    # 2. Perform Lookup
    print("\nPerforming Semantic Lookup...")
    try:
        result = await lookup_cache(query, tenant_id)
        if result:
            print(f"✓ HIT! Cache ID: {result.cache_id}")
            print(f"  Similarity: {result.similarity:.4f}")
            print(f"  SQL: {result.value}")
        else:
            print("✗ MISS. No matching entry found with similarity >= 0.90")

            # Debug: Check best similarity even if < 0.90
            embedding = RagEngine.embed_text(query)
            async with Database.get_connection(tenant_id) as conn:
                row = await conn.fetchrow(
                    """
                    SELECT cache_id, (1 - (query_embedding <=> $1)) as similarity
                    FROM semantic_cache
                    WHERE tenant_id = $2 AND cache_type='sql'
                    ORDER BY similarity DESC LIMIT 1
                """,
                    str(embedding),
                    tenant_id,
                )

                if row:
                    print(
                        f"  Best available match was ID {row['cache_id']} "
                        f"with similarity {row['similarity']:.4f}"
                    )
                else:
                    print("  No entries found to compare.")

    except Exception as e:
        print(f"Error during lookup: {e}")

    await Database.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug semantic cache")
    parser.add_argument("query", type=str, help="The natural language query")
    parser.add_argument("--tenant", type=int, default=1, help="Tenant ID (default: 1)")

    args = parser.parse_args()

    asyncio.run(check_cache(args.query, args.tenant))

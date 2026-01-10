import json
import time

import pytest
from mcp_server.config.database import Database
from mcp_server.dal.factory import get_cache_store
from mcp_server.tools.semantic import get_semantic_subgraph

TEST_QUERY = "Integration Test: Show me the hierarchy of the payment system"
TENANT_ID = 1


@pytest.mark.asyncio
async def test_cache_write_through():
    """Verify cache write-through and performance gain."""
    # Initialize DB connection pool
    await Database.init()
    try:
        cache = get_cache_store()

        # Pre-clean: Ensure no leftover cache exists
        await cache.delete_entry(TEST_QUERY, TENANT_ID)

        print("\n--- Cold Run ---")
        start_cold = time.perf_counter()
        # Call with tenant_id to enable caching
        result_cold_json = await get_semantic_subgraph(TEST_QUERY, TENANT_ID)
        end_cold = time.perf_counter()
        time_cold = end_cold - start_cold

        assert result_cold_json is not None
        result_cold = json.loads(result_cold_json)
        assert "nodes" in result_cold
        assert "error" not in result_cold
        print(f"Cold Time: {time_cold:.4f}s")

        print("\n--- Warm Run ---")
        start_warm = time.perf_counter()
        # Should hit cache
        result_warm_json = await get_semantic_subgraph(TEST_QUERY, TENANT_ID)
        end_warm = time.perf_counter()
        time_warm = end_warm - start_warm

        print(f"Warm Time: {time_warm:.4f}s")

        # Assertions
        assert (
            result_warm_json == result_cold_json
        ), "Warm result should match cold result exactly (cached)"

        # Performance check
        speedup = time_cold / time_warm if time_warm > 0 else 999
        print(f"Speedup: {speedup:.2f}x")

        # We expect significant speedup, but CI/local variance exists.
        # Strict requirement: Warm must be faster.
        assert time_warm < time_cold, "Cache hit should be faster than cold run"

    finally:
        # Teardown: Clean up
        try:
            await cache.delete_entry(TEST_QUERY, TENANT_ID)
        except Exception:
            pass
        await Database.close()


@pytest.mark.asyncio
async def test_cache_pruning():
    """Verify pruning of legacy entries."""
    # Initialize DB (if not already init by previous test, but pytest async scope usually
    # requires re-init per function or fixture. We call init here safely.)
    try:
        await Database.init()
    except Exception:
        pass  # Already initialized potentially

    try:
        # 1. Insert a legacy row (v0_legacy)
        legacy_query = "Legacy Query Integration Test"
        # We need to construct a dummy embedding
        dummy_embedding = [0.1] * 384
        from mcp_server.dal.postgres.common import _format_vector

        pg_vector = _format_vector(dummy_embedding)

        insert_sql = """
            INSERT INTO semantic_cache (
                tenant_id, user_query, query_embedding, generated_sql, schema_version, cache_type
            )
            VALUES ($1, $2, $3, $4, 'v0_legacy', 'sql')
        """
        async with Database.get_connection(TENANT_ID) as conn:
            await conn.execute(insert_sql, TENANT_ID, legacy_query, pg_vector, "{}")

        # Verify insertion
        check_sql = "SELECT count(*) FROM semantic_cache WHERE schema_version = 'v0_legacy'"
        async with Database.get_connection(TENANT_ID) as conn:
            count_before = await conn.fetchval(check_sql)

        assert count_before >= 1, "Failed to insert legacy test row"
        print(f"Inserted {count_before} legacy rows.")

        # 2. Call Prune
        from mcp_server.services.cache_service import prune_legacy_entries

        deleted_count = await prune_legacy_entries()

        # 3. Assert gone
        async with Database.get_connection(TENANT_ID) as conn:
            count_after = await conn.fetchval(check_sql)

        assert count_after == 0, "Legacy rows still exist after pruning"
        assert deleted_count >= 1, "Prune function reported 0 deletions"
        print(f"✓ Pruning verification passed. Deleted {deleted_count} entries.")

    finally:
        await Database.close()

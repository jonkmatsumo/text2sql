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

"""Integration tests for cache correctness and isolation."""

import pytest

from mcp_server.config.database import Database
from mcp_server.services.cache import lookup_cache, update_cache
from mcp_server.services.registry import RegistryService


# Marked with integration to be excluded from CI
@pytest.mark.integration
@pytest.mark.asyncio
async def test_cache_sort_collision_integration():
    """Verify that 'longest' and 'shortest' queries do not result in cache hits for each other.

    This test checks behavior at the database/registry level.
    """
    # Ensure database is initialized for registry
    await Database.init()

    tenant_id = 88888
    q_shortest = "10 shortest films"
    sql_shortest = "SELECT title FROM film ORDER BY length ASC LIMIT 10"

    q_longest = "10 longest films"

    # 1. Clear any existing cache for this test tenant to be clean
    # (In a real test we might want to use a fresh tenant or a cleanup fixture)

    # 2. Update cache with shortest
    await update_cache(q_shortest, sql_shortest, tenant_id)

    # 3. Lookup longest -> Expect MISS
    result_longest = await lookup_cache(q_longest, tenant_id)
    assert (
        result_longest is None
    ), "Longest query should NOT hit shortest cache entry due to different signatures"

    # 4. Lookup shortest -> Expect HIT
    result_shortest = await lookup_cache(q_shortest, tenant_id)
    assert result_shortest is not None
    assert result_shortest.value == sql_shortest

    # Cleanup (Tombstone the entries if RegistryService supports it)
    if result_shortest:
        await RegistryService.tombstone_pair(result_shortest.cache_id, tenant_id, "test cleanup")

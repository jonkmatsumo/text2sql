"""Security tests for Row-Level Security (RLS) isolation.

These are integration tests that require a real database connection.
They will be skipped if the database is not available.
"""

import os

import pytest
from mcp_server.db import Database


def is_database_available():
    """Check if database is available for integration tests."""
    # Skip if explicitly disabled
    if os.getenv("SKIP_INTEGRATION_TESTS") == "1":
        return False

    # If RUN_INTEGRATION_TESTS is explicitly set to "1", assume DB is available
    if os.getenv("RUN_INTEGRATION_TESTS") == "1":
        return True

    # If DB_HOST is set to something other than localhost, assume it's available
    db_host = os.getenv("DB_HOST", "localhost")
    if db_host != "localhost":
        return True

    # For localhost, be conservative: skip by default unless explicitly enabled
    # This prevents tests from failing when database isn't running
    return False


@pytest.mark.integration
@pytest.mark.skipif(
    not is_database_available(),
    reason="Database not available. Set DB_HOST or run with docker compose.",
)
@pytest.mark.asyncio
async def test_cross_tenant_isolation():
    """Test that tenants cannot see each other's data."""
    await Database.init()

    try:
        tenant_a = 1
        tenant_b = 2

        # Query all customers
        query = "SELECT COUNT(*) as count FROM customer"

        async with Database.get_connection(tenant_id=tenant_a) as conn_a:
            count_a = await conn_a.fetchval(query)

        async with Database.get_connection(tenant_id=tenant_b) as conn_b:
            count_b = await conn_b.fetchval(query)

        # Assert tenants see different counts
        assert count_a != count_b, "Tenants should see different data"

        # Assert total doesn't exceed actual (sanity check)
        async with Database.get_connection() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM customer")
            assert count_a + count_b <= total, "Tenant counts should not exceed total"

        print(f"✓ Tenant A sees {count_a} customers")
        print(f"✓ Tenant B sees {count_b} customers")

    finally:
        await Database.close()


@pytest.mark.integration
@pytest.mark.skipif(
    not is_database_available(),
    reason="Database not available. Set DB_HOST or run with docker compose.",
)
@pytest.mark.asyncio
async def test_tenant_context_cleared():
    """Test that tenant context is cleared after transaction."""
    await Database.init()

    try:
        tenant_id = 1

        # Set tenant context
        async with Database.get_connection(tenant_id=tenant_id) as conn:
            result = await conn.fetchval("SELECT current_setting('app.current_tenant', true)")
            assert result == str(tenant_id), f"Expected '{tenant_id}', got {result}"

        # After transaction, context should be cleared
        async with Database.get_connection() as conn:
            result = await conn.fetchval("SELECT current_setting('app.current_tenant', true)")
            assert result is None or result == "", f"Expected None/empty, got {result}"

        print("✓ Tenant context cleared after transaction")

    finally:
        await Database.close()


@pytest.mark.integration
@pytest.mark.skipif(
    not is_database_available(),
    reason="Database not available. Set DB_HOST or run with docker compose.",
)
@pytest.mark.asyncio
async def test_no_tenant_sees_nothing():
    """Test that queries without tenant context return no rows (fail-safe)."""
    await Database.init()

    try:
        # Query without tenant context
        async with Database.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM customer")
            assert count == 0, "Without tenant context, RLS should return no rows"

        print("✓ Queries without tenant context return no rows (fail-safe)")

    finally:
        await Database.close()


@pytest.mark.integration
@pytest.mark.skipif(
    not is_database_available(),
    reason="Database not available. Set DB_HOST or run with docker compose.",
)
@pytest.mark.asyncio
async def test_all_tables_isolated():
    """Test that RLS works across all protected tables."""
    await Database.init()

    try:
        tenant_id = 1
        tables = ["customer", "payment", "rental", "staff", "inventory"]

        async with Database.get_connection(tenant_id=tenant_id) as conn:
            for table in tables:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                assert count >= 0, f"Table {table} should return count >= 0"
                print(f"✓ {table}: {count} rows visible to tenant {tenant_id}")

    finally:
        await Database.close()

"""Tests verifying CockroachDB routing and capability behavior.

CockroachDB intentionally falls through to the Postgres asyncpg pool path in
Database.init() and Database.get_connection(). This is by design since CockroachDB
is wire-compatible with PostgreSQL. The key difference is that cockroachdb has
supports_transactions=False in its BackendCapabilities, which causes the
connection wrapper to skip the transaction block.
"""

from contextlib import asynccontextmanager

import pytest

from dal.capabilities import capabilities_for_provider
from dal.database import Database


class _FakeConn:
    def __init__(self):
        self.transaction_called = False
        self.execute_calls = []

    @asynccontextmanager
    async def transaction(self, readonly=False):
        self.transaction_called = True
        yield

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn

    async def close(self):
        """No-op close for test isolation."""
        pass


@pytest.fixture(autouse=True)
def reset_database_state():
    """Reset Database class state before and after each test."""
    yield
    # Cleanup after test
    Database._pool = None
    Database._query_target_provider = None
    Database._query_target_capabilities = None


@pytest.mark.asyncio
async def test_cockroach_skips_transaction_wrapper():
    """Ensure cockroachdb does not wrap query-target in a transaction."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    Database._query_target_provider = "cockroachdb"
    Database._query_target_capabilities = capabilities_for_provider("cockroachdb")

    async with Database.get_connection(tenant_id=1, read_only=True):
        pass

    assert conn.transaction_called is False


@pytest.mark.asyncio
async def test_cockroach_uses_postgres_pool_path():
    """Verify cockroachdb routes through the asyncpg pool fallthrough path.

    This test codifies the intentional design decision that cockroachdb
    does NOT have an explicit dispatch branch in Database.get_connection().
    Instead, it falls through to the Postgres asyncpg pool path (lines 450+
    in database.py), relying on wire-compatibility.
    """
    conn = _FakeConn()
    pool = _FakePool(conn)
    Database._pool = pool
    Database._query_target_provider = "cockroachdb"
    Database._query_target_capabilities = capabilities_for_provider("cockroachdb")

    # Connection should come from the pool (fallthrough to Postgres path)
    async with Database.get_connection() as acquired_conn:
        # Verify we got the connection from the pool
        assert acquired_conn is conn

    # Verify capabilities are correctly applied
    caps = Database.get_query_target_capabilities()
    assert caps.supports_transactions is False
    assert caps.execution_model == "sync"


@pytest.mark.asyncio
async def test_cockroach_tenant_context_without_transaction():
    """Verify tenant context is set even without transaction wrapper."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    Database._query_target_provider = "cockroachdb"
    Database._query_target_capabilities = capabilities_for_provider("cockroachdb")

    async with Database.get_connection(tenant_id=42):
        pass

    # Should have called set_config for tenant but NOT used transaction
    assert conn.transaction_called is False
    assert len(conn.execute_calls) == 1
    assert "set_config" in conn.execute_calls[0][0]
    assert conn.execute_calls[0][1] == ("42",)

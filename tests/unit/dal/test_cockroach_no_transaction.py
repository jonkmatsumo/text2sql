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

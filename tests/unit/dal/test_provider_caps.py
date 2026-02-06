"""Tests for provider cap metadata propagation."""

import pytest

from dal.tracing import TracedAsyncpgConnection


class _FakeAsyncpgConn:
    async def fetch(self, sql: str, *params):
        _ = sql, params
        return [{"id": 1}, {"id": 2}]


@pytest.mark.asyncio
async def test_traced_asyncpg_connection_sets_provider_cap_reason():
    """Provider caps should map to PROVIDER_CAP metadata."""
    conn = TracedAsyncpgConnection(
        _FakeAsyncpgConn(), provider="postgres", execution_model="sync", max_rows=1
    )
    rows = await conn.fetch("SELECT 1")
    assert rows == [{"id": 1}]
    assert conn.last_truncated is True
    assert conn.last_truncated_reason == "PROVIDER_CAP"

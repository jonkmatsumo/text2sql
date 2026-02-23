from contextlib import asynccontextmanager

import pytest

from dal.capabilities import capabilities_for_provider
from dal.database import Database


class _FakeConn:
    def __init__(self):
        self.execute_calls = []
        self.events = []
        self.transaction_readonly = None

    @asynccontextmanager
    async def transaction(self, readonly=False):
        self.transaction_readonly = readonly
        yield

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        self.events.append(("execute", sql))

    async def fetch(self, sql, *args):
        self.events.append(("fetch", sql))
        return [{"ok": 1}]


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self._conn


@pytest.fixture(autouse=True)
def _reset_database_state():
    Database._pool = None
    Database._query_target_provider = "postgres"
    Database._query_target_capabilities = capabilities_for_provider("postgres")
    Database._query_target_sync_max_rows = 0
    yield
    Database._pool = None
    Database._query_target_provider = "postgres"
    Database._query_target_capabilities = None
    Database._query_target_sync_max_rows = 0


@pytest.mark.asyncio
async def test_postgres_restricted_session_disabled_by_default():
    """Restricted session guardrails should be a no-op by default."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)

    async with Database.get_connection(read_only=True):
        pass

    assert conn.transaction_readonly is True
    assert conn.execute_calls == []


@pytest.mark.asyncio
async def test_postgres_restricted_session_applies_expected_settings(monkeypatch):
    """Restricted mode should apply local read-only and timeout safeguards."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)

    monkeypatch.setenv("POSTGRES_RESTRICTED_SESSION_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_RESTRICTED_STATEMENT_TIMEOUT_MS", "11000")
    monkeypatch.setenv("POSTGRES_RESTRICTED_LOCK_TIMEOUT_MS", "7000")
    monkeypatch.setenv("POSTGRES_RESTRICTED_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", "9000")
    monkeypatch.setenv("POSTGRES_RESTRICTED_SEARCH_PATH", "public")

    async with Database.get_connection(read_only=True):
        pass

    assert conn.execute_calls == [
        ("SELECT set_config('default_transaction_read_only', 'on', true)", ()),
        ("SELECT set_config('statement_timeout', $1, true)", ("11000ms",)),
        ("SELECT set_config('lock_timeout', $1, true)", ("7000ms",)),
        ("SELECT set_config('idle_in_transaction_session_timeout', $1, true)", ("9000ms",)),
        ("SELECT set_config('search_path', $1, true)", ("public",)),
    ]


@pytest.mark.asyncio
async def test_postgres_restricted_session_skips_when_not_read_only(monkeypatch):
    """Restricted session settings should not run for non-read-only usage."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_RESTRICTED_SESSION_ENABLED", "true")

    async with Database.get_connection(read_only=False):
        pass

    assert conn.transaction_readonly is False
    assert conn.execute_calls == []


@pytest.mark.asyncio
async def test_postgres_execution_role_set_local_role_before_query(monkeypatch):
    """Execution role should be set at transaction start before SQL fetches."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    async with Database.get_connection(read_only=True) as wrapped_conn:
        await wrapped_conn.fetch("SELECT 1 AS ok")

    set_role_sql = 'SET LOCAL ROLE "text2sql_readonly"'
    assert (set_role_sql, ()) in conn.execute_calls

    set_role_index = conn.events.index(("execute", set_role_sql))
    fetch_index = conn.events.index(("fetch", "SELECT 1 AS ok"))
    assert set_role_index < fetch_index


@pytest.mark.asyncio
async def test_postgres_execution_role_enabled_without_role_is_noop(monkeypatch):
    """Role switching should not be attempted when role env is empty."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")

    async with Database.get_connection(read_only=True):
        pass

    assert not any("SET LOCAL ROLE" in sql for sql, _ in conn.execute_calls)

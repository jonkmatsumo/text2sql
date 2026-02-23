from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

from dal.capabilities import capabilities_for_provider
from dal.database import Database
from dal.session_guardrails import PostgresSessionGuardrailSettings, SessionGuardrailPolicyError


class _FakeConn:
    def __init__(self):
        self.execute_calls = []
        self.events = []
        self.fetchrow_calls = []
        self.fetchrow_result = {"dblink_installed": False, "dblink_accessible": False}
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

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        self.events.append(("fetchrow", sql))
        return self.fetchrow_result


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
    Database._postgres_extension_capability_cache = {}
    Database._postgres_extension_warning_emitted = set()
    Database._postgres_session_guardrail_settings = None
    yield
    Database._pool = None
    Database._query_target_provider = "postgres"
    Database._query_target_capabilities = None
    Database._query_target_sync_max_rows = 0
    Database._postgres_extension_capability_cache = {}
    Database._postgres_extension_warning_emitted = set()
    Database._postgres_session_guardrail_settings = None


@pytest.mark.asyncio
async def test_postgres_restricted_session_disabled_by_default():
    """Restricted session guardrails should be a no-op by default."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)

    async with Database.get_connection(read_only=True):
        pass

    assert conn.transaction_readonly is True
    assert conn.execute_calls == [
        ("RESET ROLE", ()),
        ("RESET ALL", ()),
    ]


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
        ("RESET ROLE", ()),
        ("RESET ALL", ()),
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
    assert conn.execute_calls == [
        ("RESET ROLE", ()),
        ("RESET ALL", ()),
    ]


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
async def test_postgres_execution_role_enabled_without_role_fails_closed(monkeypatch):
    """Execution role mode without role name should fail closed."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")

    with pytest.raises(SessionGuardrailPolicyError) as exc_info:
        async with Database.get_connection(read_only=True):
            pass
    assert exc_info.value.outcome == "SESSION_GUARDRAIL_MISCONFIGURED"


@pytest.mark.asyncio
async def test_postgres_execution_role_dblink_probe_cached(monkeypatch):
    """Dangerous extension capability probe should be cached per execution role."""
    conn = _FakeConn()
    conn.fetchrow_result = {"dblink_installed": True, "dblink_accessible": False}
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    async with Database.get_connection(read_only=True):
        pass
    async with Database.get_connection(read_only=True):
        pass

    assert len(conn.fetchrow_calls) == 1


@pytest.mark.asyncio
async def test_postgres_execution_role_dblink_accessible_emits_warning_signal(monkeypatch):
    """Accessible dblink should set telemetry attributes and emit warning metric."""
    conn = _FakeConn()
    conn.fetchrow_result = {"dblink_installed": True, "dblink_accessible": True}
    Database._pool = _FakePool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    mock_span = MagicMock()
    mock_span.is_recording.return_value = True

    with (
        patch("dal.database.trace.get_current_span", return_value=mock_span),
        patch("dal.database.mcp_metrics.add_counter") as mock_counter,
    ):
        async with Database.get_connection(read_only=True):
            pass

    mock_span.set_attribute.assert_any_call("db.postgres.execution_role", "text2sql_readonly")
    mock_span.set_attribute.assert_any_call("db.postgres.extension.dblink.installed", True)
    mock_span.set_attribute.assert_any_call("db.postgres.extension.dblink.accessible", True)
    mock_counter.assert_called_once()


@pytest.mark.asyncio
async def test_postgres_restricted_session_capability_mismatch_fails_closed():
    """Capability mismatch should raise a deterministic policy exception."""
    conn = _FakeConn()
    Database._pool = _FakePool(conn)
    Database._postgres_session_guardrail_settings = PostgresSessionGuardrailSettings(
        restricted_session_enabled=True,
        execution_role_enabled=False,
        execution_role_name=None,
    )

    mock_caps = MagicMock()
    mock_caps.supports_transactions = True
    mock_caps.execution_model = "sync"
    mock_caps.supports_restricted_session = False
    mock_caps.supports_execution_role = False
    Database._query_target_capabilities = mock_caps

    with pytest.raises(
        SessionGuardrailPolicyError,
        match="Restricted session guardrails are not supported",
    ):
        async with Database.get_connection(read_only=True):
            pass

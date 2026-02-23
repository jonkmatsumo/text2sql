"""Determinism and idempotence tests for Postgres session guardrails."""

from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from dal.capabilities import capabilities_for_provider
from dal.database import Database
from dal.session_guardrails import PostgresSessionGuardrailSettings, SessionGuardrailPolicyError


class _DeterministicConn:
    def __init__(self):
        self.execute_calls = []
        self.fetch_calls = []
        self.fetchrow_calls = []
        self._transaction_counter = 0
        self._current_transaction_id = None
        self.transaction_events = {}

    @asynccontextmanager
    async def transaction(self, readonly=False):
        tx_id = self._transaction_counter
        self._transaction_counter += 1
        self._current_transaction_id = tx_id
        self.transaction_events[tx_id] = [("tx_start", str(bool(readonly)).lower())]
        try:
            yield
        finally:
            self.transaction_events[tx_id].append(("tx_end", "true"))
            self._current_transaction_id = None

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        if self._current_transaction_id is not None:
            self.transaction_events[self._current_transaction_id].append(("execute", sql))

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        if self._current_transaction_id is not None:
            self.transaction_events[self._current_transaction_id].append(("fetch", sql))
        return [{"ok": 1}]

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        if self._current_transaction_id is not None:
            self.transaction_events[self._current_transaction_id].append(("fetchrow", sql))
        return {"dblink_installed": False, "dblink_accessible": False}


class _DeterministicPool:
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
async def test_guardrail_metadata_deterministic_across_repeated_execution(monkeypatch):
    """Repeated reads should emit identical bounded session-guardrail metadata."""
    conn = _DeterministicConn()
    Database._pool = _DeterministicPool(conn)
    monkeypatch.setenv("POSTGRES_RESTRICTED_SESSION_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    metadata_runs = []
    for _ in range(2):
        async with Database.get_connection(read_only=True) as wrapped_conn:
            await wrapped_conn.fetch("SELECT 1 AS ok")
            metadata_runs.append(dict(getattr(wrapped_conn, "session_guardrail_metadata", {})))

    assert metadata_runs[0] == metadata_runs[1]
    assert metadata_runs[0]["session_guardrail_applied"] is True
    assert metadata_runs[0]["session_guardrail_outcome"] == "SESSION_GUARDRAIL_APPLIED"
    assert metadata_runs[0]["execution_role_applied"] is True
    assert metadata_runs[0]["execution_role_name"] == "text2sql_readonly"
    assert metadata_runs[0]["restricted_session_mode"] == "set_local_config"


@pytest.mark.asyncio
async def test_execution_role_set_local_role_once_per_transaction(monkeypatch):
    """SET LOCAL ROLE should execute exactly once per transaction before SQL fetches."""
    conn = _DeterministicConn()
    Database._pool = _DeterministicPool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    for _ in range(2):
        async with Database.get_connection(read_only=True) as wrapped_conn:
            await wrapped_conn.fetch("SELECT 1 AS ok")

    role_sql = 'SET LOCAL ROLE "text2sql_readonly"'
    assert sum(1 for sql, _ in conn.execute_calls if sql == role_sql) == 2

    for tx_id in sorted(conn.transaction_events):
        tx_events = conn.transaction_events[tx_id]
        role_events = [event for event in tx_events if event == ("execute", role_sql)]
        assert len(role_events) == 1

        role_index = tx_events.index(("execute", role_sql))
        fetch_index = tx_events.index(("fetch", "SELECT 1 AS ok"))
        assert role_index < fetch_index


@pytest.mark.asyncio
async def test_unknown_provider_with_guardrails_enabled_fails_closed():
    """Unknown providers must fail closed when session guardrails are enabled."""
    conn = _DeterministicConn()
    Database._pool = _DeterministicPool(conn)
    Database._query_target_provider = "unknown-provider"
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

    with pytest.raises(SessionGuardrailPolicyError) as exc_info:
        async with Database.get_connection(read_only=True):
            pass
    assert exc_info.value.outcome == "SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER"


@pytest.mark.asyncio
async def test_execution_role_enabled_without_role_name_fails_closed(monkeypatch):
    """Missing role name should fail closed with deterministic misconfiguration outcome."""
    conn = _DeterministicConn()
    Database._pool = _DeterministicPool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.delenv("POSTGRES_EXECUTION_ROLE", raising=False)

    with pytest.raises(SessionGuardrailPolicyError) as exc_info:
        async with Database.get_connection(read_only=True):
            pass
    assert exc_info.value.outcome == "SESSION_GUARDRAIL_MISCONFIGURED"

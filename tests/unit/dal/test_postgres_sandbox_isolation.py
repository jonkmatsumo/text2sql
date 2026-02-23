"""Sandbox isolation regression tests for pooled Postgres executions."""

from contextlib import asynccontextmanager

import pytest

from dal.capabilities import capabilities_for_provider
from dal.database import Database


class _IsolationConn:
    def __init__(self):
        self.execute_calls = []
        self.fetch_calls = []
        self.fetchrow_calls = []
        self.fetchval_calls = []
        self.transaction_events = []
        self._current_tx_index = None
        self.fail_next_fetch = False
        self.settings = {
            "role": "none",
            "search_path": "public",
            "statement_timeout": "0",
            "lock_timeout": "0",
            "idle_in_transaction_session_timeout": "0",
        }

    @asynccontextmanager
    async def transaction(self, readonly=False):
        tx_index = len(self.transaction_events)
        self.transaction_events.append(
            [("tx_start_role", self.settings["role"]), ("tx_readonly", readonly)]
        )
        self._current_tx_index = tx_index
        try:
            yield
        except Exception as exc:
            self.transaction_events[tx_index].append(("tx_exit_exc", type(exc).__name__))
            raise
        else:
            self.transaction_events[tx_index].append(("tx_exit_ok", "ok"))
        finally:
            self._current_tx_index = None

    def _append_tx_event(self, kind: str, value: str) -> None:
        if self._current_tx_index is None:
            return
        self.transaction_events[self._current_tx_index].append((kind, value))

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        self._append_tx_event("execute", sql)
        if sql.startswith("SET LOCAL ROLE "):
            self.settings["role"] = "text2sql_readonly"
        elif sql == "RESET ROLE":
            self.settings["role"] = "none"
        elif sql == "RESET ALL":
            self.settings["search_path"] = "public"
            self.settings["statement_timeout"] = "0"
            self.settings["lock_timeout"] = "0"
            self.settings["idle_in_transaction_session_timeout"] = "0"
        return "OK"

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        self._append_tx_event("fetch", sql)
        if self.fail_next_fetch:
            self.fail_next_fetch = False
            raise RuntimeError("simulated execution failure")
        return [{"ok": 1, "active_role": self.settings["role"]}]

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        self._append_tx_event("fetchrow", sql)
        return {"dblink_installed": False, "dblink_accessible": False}

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        if "current_setting('role'" in sql:
            return self.settings["role"]
        if "current_setting('search_path'" in sql:
            return self.settings["search_path"]
        if "current_setting('statement_timeout'" in sql:
            return self.settings["statement_timeout"]
        if "current_setting('lock_timeout'" in sql:
            return self.settings["lock_timeout"]
        if "current_setting('idle_in_transaction_session_timeout'" in sql:
            return self.settings["idle_in_transaction_session_timeout"]
        return None


class _IsolationPool:
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
async def test_sequential_reuse_observes_clean_state(monkeypatch):
    """Second execution on a reused connection should observe clean sandbox state."""
    conn = _IsolationConn()
    Database._pool = _IsolationPool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    metadata_runs = []
    for _ in range(2):
        wrapped_conn = None
        async with Database.get_connection(read_only=True) as wrapped_conn:
            rows = await wrapped_conn.fetch("SELECT 1 AS ok")
            assert rows[0]["active_role"] == "text2sql_readonly"
        metadata_runs.append(dict(getattr(wrapped_conn, "postgres_sandbox_metadata", {})))

    assert metadata_runs[0] == metadata_runs[1]
    assert metadata_runs[0]["sandbox_applied"] is True
    assert metadata_runs[0]["sandbox_rollback"] is False
    assert metadata_runs[0]["sandbox_failure_reason"] == "NONE"
    assert conn.settings["role"] == "none"
    assert sum(1 for sql, _ in conn.execute_calls if sql == "RESET ROLE") == 2
    assert sum(1 for sql, _ in conn.execute_calls if sql == "RESET ALL") == 2


@pytest.mark.asyncio
async def test_failure_in_first_execution_does_not_contaminate_second(monkeypatch):
    """Rollback from first failure must leave a clean state for the next execution."""
    conn = _IsolationConn()
    Database._pool = _IsolationPool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    conn.fail_next_fetch = True
    with pytest.raises(RuntimeError) as exc_info:
        async with Database.get_connection(read_only=True) as wrapped_conn:
            await wrapped_conn.fetch("SELECT 1 AS ok")
    first_error_metadata = dict(getattr(exc_info.value, "postgres_sandbox_metadata", {}))
    assert first_error_metadata["sandbox_applied"] is True
    assert first_error_metadata["sandbox_rollback"] is True
    assert first_error_metadata["sandbox_failure_reason"] == "QUERY_ERROR"

    wrapped_conn = None
    async with Database.get_connection(read_only=True) as wrapped_conn:
        rows = await wrapped_conn.fetch("SELECT 1 AS ok")
        assert rows[0]["active_role"] == "text2sql_readonly"
    second_metadata = dict(getattr(wrapped_conn, "postgres_sandbox_metadata", {}))
    assert second_metadata["sandbox_applied"] is True
    assert second_metadata["sandbox_rollback"] is False
    assert second_metadata["sandbox_failure_reason"] == "NONE"
    assert conn.settings["role"] == "none"


@pytest.mark.asyncio
async def test_execution_role_not_sticky_across_transactions(monkeypatch):
    """SET LOCAL ROLE should execute once per transaction and never be sticky."""
    conn = _IsolationConn()
    Database._pool = _IsolationPool(conn)
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE_ENABLED", "true")
    monkeypatch.setenv("POSTGRES_EXECUTION_ROLE", "text2sql_readonly")

    for _ in range(2):
        async with Database.get_connection(read_only=True) as wrapped_conn:
            await wrapped_conn.fetch("SELECT 1 AS ok")

    role_sql = 'SET LOCAL ROLE "text2sql_readonly"'
    assert sum(1 for sql, _ in conn.execute_calls if sql == role_sql) == 2
    start_roles = []
    for tx_events in conn.transaction_events:
        tx_start_role = [value for key, value in tx_events if key == "tx_start_role"][0]
        start_roles.append(tx_start_role)
        role_events = [event for event in tx_events if event == ("execute", role_sql)]
        assert len(role_events) == 1
    assert start_roles == ["none", "none"]

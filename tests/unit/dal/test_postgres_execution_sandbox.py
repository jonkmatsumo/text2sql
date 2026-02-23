"""Unit tests for PostgresExecutionSandbox transaction behavior."""

import pytest

from dal.postgres_sandbox import PostgresExecutionSandbox, PostgresSandboxStateError


class _FakeTransaction:
    def __init__(self):
        self.entered = 0
        self.exited = 0
        self.exit_args = None

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exited += 1
        self.exit_args = (exc_type, exc, tb)
        return False


class _FakeConn:
    def __init__(self):
        self.transaction_calls = []
        self.transactions = []
        self.execute_calls = []
        self.fetchval_calls = []
        self.settings = {
            "role": "none",
            "search_path": "public",
            "statement_timeout": "0",
            "lock_timeout": "0",
            "idle_in_transaction_session_timeout": "0",
        }

    def transaction(self, readonly=False):
        self.transaction_calls.append(readonly)
        tx = _FakeTransaction()
        self.transactions.append(tx)
        return tx

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        if sql == "RESET ROLE":
            self.settings["role"] = "none"
        elif sql == "RESET ALL":
            self.settings["search_path"] = "public"
            self.settings["statement_timeout"] = "0"
            self.settings["lock_timeout"] = "0"
            self.settings["idle_in_transaction_session_timeout"] = "0"
        return "OK"

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


@pytest.mark.asyncio
async def test_postgres_execution_sandbox_commits_on_success():
    """Sandbox should commit transaction when no exception occurs."""
    conn = _FakeConn()

    async with PostgresExecutionSandbox(conn, read_only=True) as sandbox:
        pass

    assert conn.transaction_calls == [True]
    assert conn.transactions[0].entered == 1
    assert conn.transactions[0].exited == 1
    assert conn.transactions[0].exit_args[0] is None
    assert ("RESET ROLE", ()) in conn.execute_calls
    assert ("RESET ALL", ()) in conn.execute_calls
    assert sandbox.result.committed is True
    assert sandbox.result.rolled_back is False
    assert sandbox.result.state_clean is True


@pytest.mark.asyncio
async def test_postgres_execution_sandbox_rolls_back_on_exception():
    """Sandbox should force rollback semantics on exceptions."""
    conn = _FakeConn()

    with pytest.raises(RuntimeError, match="boom"):
        async with PostgresExecutionSandbox(conn, read_only=True):
            raise RuntimeError("boom")

    assert conn.transaction_calls == [True]
    assert conn.transactions[0].entered == 1
    assert conn.transactions[0].exited == 1
    assert conn.transactions[0].exit_args[0] is RuntimeError
    assert ("RESET ROLE", ()) in conn.execute_calls
    assert ("RESET ALL", ()) in conn.execute_calls


@pytest.mark.asyncio
async def test_postgres_execution_sandbox_strict_state_drift_fails(monkeypatch):
    """Strict state check should fail closed when role state remains dirty."""
    conn = _FakeConn()
    monkeypatch.setenv("POSTGRES_SANDBOX_STRICT_STATE_CHECK", "true")

    async def _no_reset_role(sql, *args):
        conn.execute_calls.append((sql, args))
        if sql == "RESET ALL":
            conn.settings["search_path"] = "public"
        return "OK"

    conn.execute = _no_reset_role

    with pytest.raises(PostgresSandboxStateError, match="connection state drift"):
        async with PostgresExecutionSandbox(conn, read_only=True):
            conn.settings["role"] = "sandbox_role"

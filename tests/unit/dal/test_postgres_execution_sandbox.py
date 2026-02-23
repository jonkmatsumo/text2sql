"""Unit tests for PostgresExecutionSandbox transaction behavior."""

import pytest

from dal.postgres_sandbox import PostgresExecutionSandbox


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

    def transaction(self, readonly=False):
        self.transaction_calls.append(readonly)
        tx = _FakeTransaction()
        self.transactions.append(tx)
        return tx


@pytest.mark.asyncio
async def test_postgres_execution_sandbox_commits_on_success():
    """Sandbox should commit transaction when no exception occurs."""
    conn = _FakeConn()

    async with PostgresExecutionSandbox(conn, read_only=True):
        pass

    assert conn.transaction_calls == [True]
    assert conn.transactions[0].entered == 1
    assert conn.transactions[0].exited == 1
    assert conn.transactions[0].exit_args[0] is None


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

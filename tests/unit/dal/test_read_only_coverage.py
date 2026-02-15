"""Unit tests for read-only enforcement across provider execution paths."""

from unittest.mock import MagicMock, patch

import pytest

from dal.database import Database
from dal.snowflake.query_target import _SnowflakeConnection


class TestDatabase(Database):
    """Test subclass to verify Database base logic."""

    _query_target_provider = "postgres"
    _query_target_sync_max_rows = 0
    # Will be set in test
    _pool = None

    @classmethod
    def get_query_target_capabilities(cls):
        """Return mock capabilities."""
        mock_caps = MagicMock()
        mock_caps.execution_model = "sync"
        return mock_caps


class DummyTransaction:
    """Dummy async context manager for transaction."""

    async def __aenter__(self):
        """Enter context."""
        return None

    async def __aexit__(self, exc_type, exc, tb):
        """Exit context."""
        pass


class DummyConn:
    """Dummy connection object."""

    def transaction(self, readonly=False):
        """Return dummy transaction."""
        return DummyTransaction()

    async def execute(self, sql, *args):
        """Execute dummy query."""
        pass


class DummyCM:
    """Dummy async context manager for get_connection/acquire."""

    def __init__(self, conn):
        """Initialize with connection."""
        self.conn = conn

    async def __aenter__(self):
        """Enter context and return connection."""
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        """Exit context."""
        pass


class DummyPool:
    """Dummy pool."""

    def __init__(self, conn):
        """Initialize with connection."""
        self.conn = conn

    def acquire(self):
        """Acquire connection context manager."""
        return DummyCM(self.conn)


class DummyQueryTarget:
    """Dummy query target."""

    def __init__(self, conn):
        """Initialize with connection."""
        self.conn = conn

    def get_connection(self, tenant_id=None, read_only=False):
        """Get connection context manager."""
        return DummyCM(self.conn)


@pytest.mark.asyncio
async def test_postgres_read_only_enforcement_without_tracing():
    """Verify that Postgres connections enforce read-only SQL even when tracing is disabled."""
    # Use full stack of dummies
    real_conn = DummyConn()
    dummy_pool = DummyPool(real_conn)
    dummy_qt = DummyQueryTarget(real_conn)

    TestDatabase.query_target = dummy_qt
    TestDatabase._pool = dummy_pool

    # Mock trace_enabled to False
    with patch("dal.tracing.trace_enabled", return_value=False):

        # Execute code under test
        async with TestDatabase.get_connection(read_only=True) as wrapper:
            from dal.tracing import TracedAsyncpgConnection

            # Assert that we got the TracedAsyncpgConnection wrapper
            assert isinstance(wrapper, TracedAsyncpgConnection)
            assert wrapper._read_only is True
            # Assert calls are delegated to our dummy
            assert wrapper._conn == real_conn

            # Further verify that executing a write query raises PermissionError
            with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
                await wrapper.execute("INSERT INTO foo VALUES (1)")

        # Verify raw connection path
        async with TestDatabase.get_connection(read_only=False) as raw_conn:
            assert raw_conn == real_conn
            assert not isinstance(raw_conn, TracedAsyncpgConnection)


@pytest.mark.asyncio
async def test_snowflake_sync_read_only_enforcement():
    """Verify that Snowflake synchronous connection wrapper enforces read-only SQL."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    wrapper = _SnowflakeConnection(
        conn=mock_conn,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=1000,
        warn_after_seconds=10,
        read_only=True,
    )

    # execute
    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("CREATE TABLE foo (id int)")

    # fetch
    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.fetch("DROP TABLE foo")

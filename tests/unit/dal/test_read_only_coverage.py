"""Unit tests for read-only enforcement across provider execution paths."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dal.database import Database
from dal.snowflake.query_target import _SnowflakeConnection


class TestDatabase(Database):
    """Test subclass to verify Database base logic."""

    _query_target_provider = "postgres"
    _query_target_sync_max_rows = 0
    _pool = MagicMock()

    @classmethod
    def get_query_target_capabilities(cls):
        """Return mock capabilities."""
        mock_caps = MagicMock()
        mock_caps.execution_model = "sync"
        return mock_caps


@pytest.mark.asyncio
async def test_postgres_read_only_enforcement_without_tracing():
    """Verify that Postgres connections enforce read-only SQL even when tracing is disabled.

    This covers the gap identified in Phase 6 where TracedAsyncpgConnection
    (and thus the guard) was bypassed if tracing was off.
    """
    # Mock the underlying connection
    mock_conn = MagicMock()
    # transaction() is a sync method that returns an async context manager
    mock_transaction_cm = MagicMock()
    mock_transaction_cm.__aenter__ = AsyncMock(return_value=None)
    mock_transaction_cm.__aexit__ = AsyncMock(return_value=None)
    mock_conn.transaction.side_effect = lambda *args, **kwargs: mock_transaction_cm

    # We need to mock the query_target.get_connection context manager
    # Database.get_connection calls cls.query_target.get_connection
    # failing to set cls.query_target would raise AttributeError

    mock_qt = MagicMock()
    mock_qt.get_connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_qt.get_connection.return_value.__aexit__ = AsyncMock(return_value=None)

    TestDatabase.query_target = mock_qt

    # Mock trace_enabled to False
    # Note: trace_enabled is imported from dal.tracing inside methods, so we patch it there.
    with patch("dal.tracing.trace_enabled", return_value=False):

        # Execute code under test
        async with TestDatabase.get_connection(read_only=True) as wrapper:
            from dal.tracing import TracedAsyncpgConnection

            # Assert that we got the TracedAsyncpgConnection wrapper
            assert isinstance(wrapper, TracedAsyncpgConnection)
            assert wrapper._read_only is True
            # Assert it wraps our mock_conn
            assert wrapper._conn == mock_conn

            # Further verify that executing a write query raises PermissionError
            # The wrapper calls enforce_read_only_sql
            with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
                await wrapper.execute("INSERT INTO foo VALUES (1)")

        # Verify that if read_only=False and tracing=False, we get the raw connection
        async with TestDatabase.get_connection(read_only=False) as raw_conn:
            # When read_only=False and trace_enabled=False, we expect the raw connection
            assert raw_conn == mock_conn
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

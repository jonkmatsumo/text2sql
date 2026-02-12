"""Provider-level read-only enforcement tests for P0 hardening."""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_athena_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure Athena wrapper rejects mutating SQL when read_only=True."""
    from dal.athena.query_target import _AthenaConnection

    executor = AsyncMock()
    wrapper = _AthenaConnection(
        executor=executor,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=100,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.fetch("DROP TABLE users")

    executor.submit.assert_not_called()


@pytest.mark.asyncio
async def test_athena_executor_blocks_mutating_sql_in_read_only_mode():
    """Ensure Athena executor rejects mutating SQL when read_only=True."""
    # Mock boto3
    import sys

    from dal.athena.executor import AthenaAsyncQueryExecutor

    sys.modules["boto3"] = MagicMock()

    executor = AthenaAsyncQueryExecutor(
        region="us-east-1",
        workgroup="test",
        output_location="s3://test",
        database="test",
        timeout_seconds=30,
        max_rows=100,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await executor.submit("DELETE FROM users")


@pytest.mark.asyncio
async def test_databricks_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure Databricks wrapper rejects mutating SQL when read_only=True."""
    from dal.databricks.query_target import _DatabricksConnection

    executor = AsyncMock()
    wrapper = _DatabricksConnection(
        executor=executor,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=100,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("INSERT INTO data VALUES (1)")

    executor.submit.assert_not_called()


@pytest.mark.asyncio
async def test_clickhouse_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure ClickHouse wrapper rejects mutating SQL when read_only=True."""
    from dal.clickhouse.query_target import _ClickHouseConnection

    conn = AsyncMock()
    wrapper = _ClickHouseConnection(
        conn=conn, query_timeout_seconds=30, max_rows=100, sync_max_rows=100, read_only=True
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.fetch("ALTER TABLE t DELETE WHERE 1=1")

    conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_athena_allows_select_in_read_only_mode():
    """Ensure Athena allows SELECT even when read_only=True."""
    from dal.async_query_executor import QueryStatus
    from dal.athena.query_target import _AthenaConnection

    executor = AsyncMock()
    executor.poll.return_value = QueryStatus.SUCCEEDED
    executor.fetch.return_value = []
    wrapper = _AthenaConnection(
        executor=executor,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=100,
        read_only=True,
    )

    # Should not raise
    await wrapper.fetch("SELECT * FROM users")
    executor.submit.assert_called_once()

"""Provider-level read-only enforcement tests."""

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_redshift_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure Redshift wrapper rejects mutating SQL when read_only=True."""
    from dal.redshift.query_target import _RedshiftConnection

    db_conn = AsyncMock()
    wrapper = _RedshiftConnection(db_conn, max_rows=100, read_only=True)

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.fetch("DELETE FROM users WHERE id = 1")

    db_conn.fetch.assert_not_called()


@pytest.mark.asyncio
async def test_bigquery_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure BigQuery wrapper rejects mutating SQL when read_only=True."""
    from dal.bigquery.query_target import _BigQueryConnection

    executor = AsyncMock()
    wrapper = _BigQueryConnection(
        executor=executor,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=100,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.fetch("INSERT INTO users VALUES (1)")

    executor.submit.assert_not_called()


@pytest.mark.asyncio
async def test_snowflake_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure Snowflake wrapper rejects mutating SQL when read_only=True."""
    pytest.importorskip("snowflake")
    from dal.snowflake.query_target import _SnowflakeConnection

    db_conn = MagicMock()
    wrapper = _SnowflakeConnection(
        db_conn,
        query_timeout_seconds=30,
        poll_interval_seconds=1,
        max_rows=100,
        warn_after_seconds=10,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("UPDATE users SET name = 'x'")


@pytest.mark.asyncio
async def test_mysql_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure MySQL wrapper rejects mutating SQL when read_only=True."""
    from dal.mysql.query_target import _MysqlConnection

    db_conn = AsyncMock()
    wrapper = _MysqlConnection(db_conn, max_rows=100, read_only=True)

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("DELETE FROM users")

    db_conn.cursor.assert_not_called()


@pytest.mark.asyncio
async def test_sqlite_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure SQLite wrapper rejects mutating SQL when read_only=True."""
    from dal.sqlite.query_target import _SqliteConnection

    db_conn = AsyncMock()
    wrapper = _SqliteConnection(db_conn, max_rows=100, read_only=True)

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("DROP TABLE users")

    db_conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_duckdb_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure DuckDB wrapper rejects mutating SQL when read_only=True."""
    from dal.duckdb.query_target import _DuckDBConnection

    db_conn = MagicMock()
    wrapper = _DuckDBConnection(
        db_conn,
        query_timeout_seconds=30,
        max_rows=100,
        sync_max_rows=100,
        read_only=True,
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("CREATE TABLE x AS SELECT 1")

    db_conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_traced_asyncpg_connection_blocks_mutating_sql_in_read_only_mode():
    """Ensure TracedAsyncpgConnection rejects mutating SQL when read_only=True."""
    from dal.tracing import TracedAsyncpgConnection

    db_conn = AsyncMock()
    wrapper = TracedAsyncpgConnection(
        db_conn, provider="postgres", execution_model="sync", read_only=True
    )

    with pytest.raises(PermissionError, match="Read-only enforcement blocked"):
        await wrapper.execute("UPDATE table SET x = 1")

    db_conn.execute.assert_not_called()

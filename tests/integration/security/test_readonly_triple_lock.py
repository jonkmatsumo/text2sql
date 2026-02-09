from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer
from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope
from dal.database import Database
from mcp_server.tools.execute_sql_query import handler as execute_sql_handler


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    """Set up the test environment with required roles and trace IDs."""
    monkeypatch.setenv("MCP_USER_ROLE", "SQL_ADMIN_ROLE")
    # Ensure tool context can be created
    monkeypatch.setenv("OTEL_TRACE_ID", "test-trace-id")


@pytest.mark.asyncio
async def test_agent_policy_rejects_mutation():
    """Verify Agent layer rejects mutation statements."""
    mutations = [
        "INSERT INTO users (name) VALUES ('hacker')",
        "UPDATE users SET name = 'hacker' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "DROP TABLE users",
        "TRUNCATE users",
    ]
    for sql in mutations:
        with pytest.raises(ValueError) as excinfo:
            PolicyEnforcer.validate_sql(sql)
        assert "Statement type not allowed" in str(excinfo.value)


@pytest.mark.asyncio
async def test_mcp_tool_rejects_mutation():
    """Verify MCP execute_sql_query tool rejects mutation statements."""
    sql = "UPDATE users SET name = 'hacker'"

    # Tool should return an error envelope
    result_json = await execute_sql_handler(
        sql_query=sql,
        tenant_id=1,
    )

    try:
        envelope = ExecuteSQLQueryResponseEnvelope.model_validate_json(result_json)
        assert envelope.error is not None
        assert "Forbidden statement type" in envelope.error.message
        assert envelope.error.category == "invalid_request"
    except Exception as e:
        print(f"DEBUG: result_json = {result_json}")
        raise e


@pytest.mark.asyncio
async def test_dal_postgres_transaction_readonly():
    """Verify DAL (Postgres) transaction is actually read-only."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    # Mock pool acquire as an async context manager
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)

    # conn.transaction(readonly=True) is an async context manager
    # It must NOT be an AsyncMock itself for the call, but return an AsyncMock for the block
    mock_transaction = AsyncMock()
    mock_conn.transaction = MagicMock(return_value=mock_transaction)

    with patch.object(Database, "_pool", mock_pool):
        with patch.object(Database, "get_query_target_capabilities") as mock_caps:
            mock_caps.return_value.supports_transactions = True

            async with Database.get_connection(read_only=True):
                pass

            mock_conn.transaction.assert_called_with(readonly=True)


@pytest.mark.asyncio
async def test_dal_sqlite_connection_readonly():
    """Verify DAL (SQLite) connection is opened in RO mode."""
    mock_conn = AsyncMock()
    with patch("aiosqlite.connect", new_callable=AsyncMock) as mock_connect:
        mock_connect.return_value = mock_conn
        with patch("dal.sqlite.query_target._resolve_sqlite_path") as mock_resolve:
            mock_resolve.return_value = ("file:test.db?mode=ro", True)

            from dal.sqlite.query_target import SqliteQueryTargetDatabase

            # We need to manually reset the state if needed
            SqliteQueryTargetDatabase._db_path = None
            await SqliteQueryTargetDatabase.init("test.db")

            async with SqliteQueryTargetDatabase.get_connection(read_only=True):
                pass

            mock_connect.assert_called()


@pytest.mark.asyncio
async def test_dal_duckdb_connection_readonly():
    """Verify DAL (DuckDB) connection is opened in RO mode."""
    with patch("duckdb.connect") as mock_connect:
        from dal.duckdb.config import DuckDBConfig
        from dal.duckdb.query_target import DuckDBQueryTargetDatabase

        config = DuckDBConfig(
            path="test.db", read_only=False, query_timeout_seconds=30, max_rows=1000
        )
        await DuckDBQueryTargetDatabase.init(config)

        async with DuckDBQueryTargetDatabase.get_connection(read_only=True):
            # In our implementation: db_read_only = read_only or cls._config.read_only
            pass

        mock_connect.assert_called_with("test.db", read_only=True)


@pytest.mark.asyncio
async def test_dal_mysql_connection_readonly():
    """Verify DAL (MySQL) connection sets session to READ ONLY."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    # Mock pool acquire
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)

    mock_cursor = AsyncMock()
    mock_conn.cursor = MagicMock()
    mock_conn.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)

    from dal.mysql.query_target import MysqlQueryTargetDatabase

    with patch.object(MysqlQueryTargetDatabase, "_pool", mock_pool):
        # init is not needed since we mock _pool
        async with MysqlQueryTargetDatabase.get_connection(read_only=True):
            pass

        mock_cursor.execute.assert_called_with("SET TRANSACTION READ ONLY")

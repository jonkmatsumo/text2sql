"""Tests for execute_sql_query tool."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from mcp_server.tools.execute_sql_query import TOOL_NAME, handler


class TestExecuteSqlQuery:
    """Tests for execute_sql_query tool."""

    def setup_method(self, method):
        """Initialize Database capabilities for tests."""
        from dal.capabilities import BackendCapabilities
        from dal.database import Database

        Database._query_target_capabilities = BackendCapabilities(
            supports_column_metadata=True,
            supports_cancel=True,
            supports_pagination=True,
            execution_model="sync",
            supports_schema_cache=False,
        )
        Database._query_target_provider = "postgres"

    @pytest.fixture(autouse=True)
    def mock_policy_enforcer(self):
        """Mock PolicyEnforcer to bypass validation."""
        with patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql"):
            yield

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "execute_sql_query"

    @pytest.mark.asyncio
    async def test_execute_sql_query_requires_tenant_id(self):
        """Test that execute_sql_query requires tenant_id."""
        result = await handler("SELECT * FROM film", tenant_id=None)

        data = json.loads(result)
        assert "error" in data
        assert data["error"]["message"] and (
            "Tenant ID" in data["error"]["message"] or "Unauthorized" in data["error"]["message"]
        )

    @pytest.mark.asyncio
    async def test_execute_sql_query_valid_select(self):
        """Test executing a valid SELECT query."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT COUNT(*) as count FROM film", tenant_id=1)

            mock_get.assert_called_once()
            mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) as count FROM film")

            data = json.loads(result)
            # New envelope structure check
            assert data["schema_version"] == "1.0"
            assert data["rows"][0]["count"] == 1000
            assert data["metadata"]["tool_version"] == "v1"
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 1

    @pytest.mark.asyncio
    async def test_execute_sql_query_include_columns_opt_in(self):
        """Opt-in include_columns returns wrapper with rows and columns."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000, "created_at": "2024-01-01T00:00:00Z"}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(
                "SELECT COUNT(*) as count, NOW() as created_at FROM film",
                tenant_id=1,
                include_columns=True,
            )

            data = json.loads(result)
            assert data["schema_version"] == "1.0"
            assert data["rows"] == mock_rows
            assert data["metadata"]["tool_version"] == "v1"
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 1
            assert data["columns"] == [
                {
                    "name": "count",
                    "type": "unknown",
                    "db_type": None,
                    "nullable": None,
                    "precision": None,
                    "scale": None,
                    "timezone": None,
                },
                {
                    "name": "created_at",
                    "type": "unknown",
                    "db_type": None,
                    "nullable": None,
                    "precision": None,
                    "scale": None,
                    "timezone": None,
                },
            ]

    @pytest.mark.asyncio
    async def test_execute_sql_query_empty_result(self):
        """Test handling empty result set."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film WHERE film_id = -1", tenant_id=1)

            data = json.loads(result)
            assert data["rows"] == []
            assert data["metadata"]["is_truncated"] is False
            assert data["metadata"]["rows_returned"] == 0

    @pytest.mark.asyncio
    async def test_execute_sql_query_size_limit(self):
        """Test enforcing 1000 row limit."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": i} for i in range(1001)]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert len(data["rows"]) == 1000
            assert data["metadata"]["is_truncated"] is True
            assert data["metadata"]["row_limit"] == 1000
            assert data["metadata"]["rows_returned"] == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_drop(self):
        """Test rejecting DROP keyword."""
        result = await handler("DROP TABLE film", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_delete(self):
        """Test rejecting DELETE keyword."""
        result = await handler("DELETE FROM film WHERE film_id = 1", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_insert(self):
        """Test rejecting INSERT keyword."""
        result = await handler("INSERT INTO film VALUES (1, 'Test')", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_update(self):
        """Test rejecting UPDATE keyword."""
        result = await handler("UPDATE film SET title = 'Test'", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_alter(self):
        """Test rejecting ALTER keyword."""
        result = await handler("ALTER TABLE film ADD COLUMN test INT", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["message"]
        assert "Forbidden statement type" in data["error"]["message"]
        assert "ALTER" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_multi_statement(self):
        """Test rejecting multiple statements."""
        result = await handler("SELECT 1; DROP TABLE film", tenant_id=1)
        data = json.loads(result)
        assert "Multi-statement queries are forbidden" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_case_insensitive(self):
        """Test case-insensitive security matching."""
        result1 = await handler("drop table film", tenant_id=1)
        data1 = json.loads(result1)
        assert "Forbidden statement type" in data1["error"]["message"]

        result2 = await handler("DeLeTe FrOm film", tenant_id=1)
        data2 = json.loads(result2)
        assert "Forbidden statement type" in data2["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_database_error(self):
        """Test handling PostgresError."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM nonexistent", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Syntax error" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_general_error(self):
        """Test handling general exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Unexpected error" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_with_params(self):
        """Test executing query with bind parameters."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": 1}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT * FROM film WHERE film_id = $1", tenant_id=1, params=[1])

            mock_conn.fetch.assert_called_once_with("SELECT * FROM film WHERE film_id = $1", 1)
            data = json.loads(result)
            assert len(data["rows"]) == 1

    @pytest.mark.asyncio
    async def test_execute_sql_query_respects_timeout_seconds(self):
        """Timeouts should return a classified timeout error."""
        mock_conn = AsyncMock()

        async def slow_fetch(*_args, **_kwargs):
            await asyncio.sleep(0.01)
            return [{"id": 1}]

        mock_conn.fetch = AsyncMock(side_effect=slow_fetch)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with (
            patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(
                "SELECT * FROM film",
                tenant_id=1,
                timeout_seconds=0.001,
            )

            data = json.loads(result)
            assert data["error"]["category"] == "timeout"

    @pytest.mark.asyncio
    async def test_execute_sql_query_max_length_exceeded(self):
        """Test rejecting a query that exceeds MCP_MAX_SQL_LENGTH."""
        with (
            patch("mcp_server.tools.execute_sql_query.get_env_int", return_value=10),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler("SELECT 1234567890", tenant_id=1)

            data = json.loads(result)
            assert data["error"]["category"] == "invalid_request"
            assert "exceeds maximum length" in data["error"]["message"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_blocked_function(self):
        """Test rejecting blocked functions like pg_sleep."""
        with patch("mcp_server.utils.auth.validate_role", return_value=None):
            result = await handler("SELECT pg_sleep(5)", tenant_id=1)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert "Forbidden function" in data["error"]["message"]
        assert "PG_SLEEP" in data["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "provider,sql_query",
        [
            ("snowflake", "UPDATE users SET name = 'x' WHERE id = 1"),
            ("bigquery", "INSERT INTO dataset.users(id) VALUES (1)"),
            ("redshift", "DELETE FROM users WHERE id = 1"),
        ],
    )
    async def test_provider_mutation_policy_rejects_before_execution(
        self, provider: str, sql_query: str
    ):
        """Mutations must be blocked deterministically before provider execution starts."""
        from dal.database import Database

        Database._query_target_provider = provider
        mock_get_connection = MagicMock()

        with (
            patch(
                "mcp_server.tools.execute_sql_query.Database.get_connection", mock_get_connection
            ),
            patch("mcp_server.utils.auth.validate_role", return_value=None),
        ):
            result = await handler(sql_query, tenant_id=7)

        data = json.loads(result)
        assert data["error"]["category"] == "invalid_request"
        assert data["error"]["provider"] == provider
        assert "Forbidden statement type" in data["error"]["message"]
        mock_get_connection.assert_not_called()

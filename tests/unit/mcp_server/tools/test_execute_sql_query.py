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

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "execute_sql_query"

    @pytest.mark.asyncio
    async def test_execute_sql_query_requires_tenant_id(self):
        """Test that execute_sql_query requires tenant_id."""
        result = await handler("SELECT * FROM film", tenant_id=None)

        error_data = json.loads(result)
        assert "error" in error_data
        assert "Tenant ID" in error_data["error"] or "Unauthorized" in error_data["error"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_valid_select(self):
        """Test executing a valid SELECT query."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler("SELECT COUNT(*) as count FROM film", tenant_id=1)

            mock_get.assert_called_once()
            mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) as count FROM film")

            data = json.loads(result)
            assert list(data.keys()) == ["rows", "metadata"]
            assert data["rows"][0]["count"] == 1000
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

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler(
                "SELECT COUNT(*) as count, NOW() as created_at FROM film",
                tenant_id=1,
                include_columns=True,
            )

            data = json.loads(result)
            assert list(data.keys()) == ["rows", "metadata", "columns"]
            assert data["rows"] == mock_rows
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

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler("SELECT * FROM film WHERE film_id = -1", tenant_id=1)

            data = json.loads(result)
            assert list(data.keys()) == ["rows", "metadata"]
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

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert list(data.keys()) == ["rows", "metadata"]
            assert len(data["rows"]) == 1000
            assert data["metadata"]["is_truncated"] is True
            assert data["metadata"]["row_limit"] == 1000
            assert data["metadata"]["rows_returned"] == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_drop(self):
        """Test rejecting DROP keyword."""
        result = await handler("DROP TABLE film", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_delete(self):
        """Test rejecting DELETE keyword."""
        result = await handler("DELETE FROM film WHERE film_id = 1", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_insert(self):
        """Test rejecting INSERT keyword."""
        result = await handler("INSERT INTO film VALUES (1, 'Test')", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_update(self):
        """Test rejecting UPDATE keyword."""
        result = await handler("UPDATE film SET title = 'Test'", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_alter(self):
        """Test rejecting ALTER keyword."""
        result = await handler("ALTER TABLE film ADD COLUMN test INT", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_case_insensitive(self):
        """Test case-insensitive security matching."""
        result1 = await handler("drop table film", tenant_id=1)
        assert "Error:" in result1

        result2 = await handler("DeLeTe FrOm film", tenant_id=1)
        assert "Error:" in result2

    @pytest.mark.asyncio
    async def test_execute_sql_query_database_error(self):
        """Test handling PostgresError."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler("SELECT * FROM nonexistent", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Syntax error" in data["error"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_general_error(self):
        """Test handling general exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler("SELECT * FROM film", tenant_id=1)

            data = json.loads(result)
            assert "error" in data
            assert "Unexpected error" in data["error"]

    @pytest.mark.asyncio
    async def test_execute_sql_query_with_params(self):
        """Test executing query with bind parameters."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": 1}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
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

        with patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get):
            result = await handler(
                "SELECT * FROM film",
                tenant_id=1,
                timeout_seconds=0.001,
            )

            data = json.loads(result)
            assert data["error_category"] == "timeout"

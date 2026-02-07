"""Tests for MCP tools."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest

from mcp_server.tools.execute_sql_query import handler as execute_sql_query_handler


@pytest.fixture
def mock_db_caps():
    """Mock database capabilities."""
    from dal.capabilities import BackendCapabilities
    from dal.database import Database

    Database._query_target_capabilities = BackendCapabilities(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_schema_cache=False,
    )
    yield
    Database._query_target_capabilities = None


@pytest.mark.asyncio
async def test_execute_sql_query_requires_tenant_id():
    """Test that execute_sql_query requires tenant_id."""
    result = await execute_sql_query_handler("SELECT * FROM film", tenant_id=None)
    data = json.loads(result)
    assert "error" in data
    assert data["error_category"] == "unsupported_capability"


@pytest.mark.asyncio
async def test_execute_sql_query_valid_select():
    """Test executing a valid SELECT query."""
    mock_conn = AsyncMock()
    mock_rows = [{"count": 1000}]
    mock_conn.fetch = AsyncMock(return_value=mock_rows)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_get = MagicMock(return_value=mock_conn)

    with (
        patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
        ) as mock_caps,
    ):
        mock_caps.return_value.supports_column_metadata = True
        mock_caps.return_value.execution_model = "sync"
        mock_caps.return_value.supports_pagination = True

        result = await execute_sql_query_handler("SELECT COUNT(*) as count FROM film", tenant_id=1)
        data = json.loads(result)
        assert data["rows"][0]["count"] == 1000


@pytest.mark.asyncio
async def test_execute_sql_query_empty_result():
    """Test handling empty result set."""
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_get = MagicMock(return_value=mock_conn)

    with (
        patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
        ) as mock_caps,
    ):
        mock_caps.return_value.supports_column_metadata = True
        mock_caps.return_value.execution_model = "sync"
        mock_caps.return_value.supports_pagination = True

        result = await execute_sql_query_handler(
            "SELECT * FROM film WHERE film_id = -1", tenant_id=1
        )
        data = json.loads(result)
        assert data["rows"] == []


@pytest.mark.asyncio
async def test_execute_sql_query_size_limit():
    """Test enforcing row limit."""
    mock_conn = AsyncMock()
    mock_rows = [{"id": i} for i in range(1001)]
    mock_conn.fetch = AsyncMock(return_value=mock_rows)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_get = MagicMock(return_value=mock_conn)

    with (
        patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
        ) as mock_caps,
    ):
        mock_caps.return_value.supports_column_metadata = True
        mock_caps.return_value.execution_model = "sync"
        mock_caps.return_value.supports_pagination = True

        result = await execute_sql_query_handler("SELECT * FROM film", tenant_id=1)
        data = json.loads(result)
        assert len(data["rows"]) == 1000
        assert data["metadata"]["is_truncated"] is True


@pytest.mark.asyncio
async def test_execute_sql_query_database_error():
    """Test handling database error."""
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_get = MagicMock(return_value=mock_conn)

    with (
        patch("mcp_server.tools.execute_sql_query.Database.get_connection", mock_get),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities"
        ) as mock_caps,
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
    ):
        mock_caps.return_value.supports_column_metadata = True
        mock_caps.return_value.execution_model = "sync"

        result = await execute_sql_query_handler("SELECT * FROM nonexistent", tenant_id=1)
        assert "error" in result
        assert "Syntax error" in result


class TestGetSemanticDefinitions:
    """Unit tests for get_semantic_definitions function."""

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_single_term(self):
        """Test retrieving definition for a single term."""
        from mcp_server.tools.get_semantic_definitions import handler

        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "High Value Customer",
                "definition": "A customer with total spend > $1000",
                "sql_logic": "total_spend > 1000",
            }
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        with patch("dal.database.Database.get_connection", return_value=mock_conn):
            result = await handler(["High Value Customer"])
            data = json.loads(result)
            assert "High Value Customer" in data
            assert (
                data["High Value Customer"]["definition"] == "A customer with total spend > $1000"
            )
            assert data["High Value Customer"]["sql_logic"] == "total_spend > 1000"


class TestSearchRelevantTables:
    """Unit tests for search_relevant_tables function."""

    @pytest.mark.asyncio
    async def test_search_relevant_tables_success(self):
        """Test successful search."""
        from dal.database import Database
        from mcp_server.tools.search_relevant_tables import handler
        from schema import TableDef

        mock_results = [{"table_name": "film", "distance": 0.1, "schema_text": "Table: film"}]
        mock_introspector = MagicMock()
        mock_introspector.get_table_def = AsyncMock(return_value=TableDef(name="film", columns=[]))

        with (
            patch(
                "mcp_server.tools.search_relevant_tables.RagEngine.embed_text",
                AsyncMock(return_value=[0.1]),
            ),
            patch(
                "mcp_server.tools.search_relevant_tables.search_similar_tables",
                AsyncMock(return_value=mock_results),
            ),
            patch.object(
                Database, "get_schema_introspector", MagicMock(return_value=mock_introspector)
            ),
        ):
            result = await handler("movies about space")
            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["table_name"] == "film"

"""Unit tests for MCP tool functions.

This module contains comprehensive tests for the core database tools.
Tests import from the new per-tool modules.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import mcp_server.tools.execute_sql_query as execute_sql_query_mod
import mcp_server.tools.get_semantic_definitions as get_semantic_definitions_mod
import mcp_server.tools.search_relevant_tables as search_relevant_tables_mod
import pytest
from mcp_server.models import ColumnDef

execute_sql_query = execute_sql_query_mod.handler
get_semantic_definitions = get_semantic_definitions_mod.handler
search_relevant_tables = search_relevant_tables_mod.handler


class TestExecuteSqlQuery:
    """Unit tests for execute_sql_query function."""

    @pytest.mark.asyncio
    async def test_execute_sql_query_requires_tenant_id(self):
        """Test that execute_sql_query requires tenant_id."""
        result = await execute_sql_query("SELECT * FROM film", tenant_id=None)

        import json

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

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):

            result = await execute_sql_query("SELECT COUNT(*) as count FROM film", tenant_id=1)

            mock_get.assert_called_once()
            mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) as count FROM film")

            import json

            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["count"] == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_empty_result(self):
        """Test handling empty result set."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film WHERE film_id = -1", tenant_id=1)

            import json

            data = json.loads(result)
            assert data == []

    @pytest.mark.asyncio
    async def test_execute_sql_query_size_limit(self):
        """Test enforcing 1000 row limit."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": i} for i in range(1001)]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film", tenant_id=1)

            import json

            data = json.loads(result)
            assert "error" in data
            assert "too large" in data["error"]
            assert "1001 rows" in data["error"]
            assert "truncated_result" in data
            assert len(data["truncated_result"]) == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_drop(self):
        """Test rejecting DROP keyword."""
        result = await execute_sql_query("DROP TABLE film", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result
        assert "DROP" in result or "drop" in result.lower()

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_delete(self):
        """Test rejecting DELETE keyword."""
        result = await execute_sql_query("DELETE FROM film WHERE film_id = 1", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_insert(self):
        """Test rejecting INSERT keyword."""
        result = await execute_sql_query("INSERT INTO film VALUES (1, 'Test')", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_update(self):
        """Test rejecting UPDATE keyword."""
        result = await execute_sql_query("UPDATE film SET title = 'Test'", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_database_error(self):
        """Test handling PostgresError."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(execute_sql_query_mod.Database, "get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM nonexistent", tenant_id=1)

            assert "Database Error:" in result
            assert "Syntax error" in result


class TestGetSemanticDefinitions:
    """Unit tests for get_semantic_definitions function."""

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_single_term(self):
        """Test retrieving definition for a single term."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "High Value Customer",
                "definition": "Customer with lifetime payments > $150",
                "sql_logic": "SUM(amount) > 150",
            }
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch.object(get_semantic_definitions_mod.Database, "get_connection", mock_get):

            result = await get_semantic_definitions(["High Value Customer"])

            mock_get.assert_called_once()

            import json

            data = json.loads(result)
            assert "High Value Customer" in data
            assert (
                data["High Value Customer"]["definition"]
                == "Customer with lifetime payments > $150"
            )

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_empty_list(self):
        """Test handling empty terms list."""
        result = await get_semantic_definitions([])

        import json

        data = json.loads(result)
        assert data == {}


class TestSearchRelevantTables:
    """Unit tests for search_relevant_tables function."""

    @pytest.mark.asyncio
    async def test_search_relevant_tables_success(self):
        """Test successful search with results."""
        mock_results = [
            {
                "table_name": "payment",
                "schema_text": "Table: payment. Columns: payment_id, amount",
                "distance": 0.1,
            },
            {
                "table_name": "customer",
                "schema_text": "Table: customer. Columns: customer_id, name",
                "distance": 0.2,
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        with patch.object(
            search_relevant_tables_mod.RagEngine, "embed_text", return_value=[0.1] * 384
        ):
            with patch.object(
                search_relevant_tables_mod, "search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch.object(
                search_relevant_tables_mod.Database, "get_schema_introspector"
            ) as mock_intro:
                mock_col = ColumnDef(name="id", data_type="int", is_nullable=False)
                mock_table_def = MagicMock()
                mock_table_def.columns = [mock_col]
                mock_table_def.foreign_keys = []
                mock_intro.return_value.get_table_def = AsyncMock(return_value=mock_table_def)
                mock_search.return_value = mock_results

                result = await search_relevant_tables("customer payment transactions", limit=5)

                mock_search.assert_called_once()

                import json

                data = json.loads(result)
                assert len(data) == 2
                assert data[0]["table_name"] == "payment"
                assert data[0]["similarity"] == 0.9

    @pytest.mark.asyncio
    async def test_search_relevant_tables_empty_result(self):
        """Test empty results handling."""
        with patch.object(
            search_relevant_tables_mod.RagEngine, "embed_text", return_value=[0.1] * 384
        ):
            with patch.object(
                search_relevant_tables_mod, "search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch.object(
                search_relevant_tables_mod.Database, "get_schema_introspector"
            ):
                mock_search.return_value = []

                result = await search_relevant_tables("nonexistent query", limit=5)

                import json

                assert json.loads(result) == []

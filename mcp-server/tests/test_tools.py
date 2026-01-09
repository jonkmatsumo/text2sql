"""Unit tests for MCP tool functions."""

from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
from mcp_server.models.database.column_def import ColumnDef
from mcp_server.tools import execute_sql_query, get_semantic_definitions, search_relevant_tables


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

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT COUNT(*) as count FROM film", tenant_id=1)

            # Verify connection was acquired (context manager called)
            mock_get.assert_called_once()

            # Verify query was executed
            mock_conn.fetch.assert_called_once_with("SELECT COUNT(*) as count FROM film")

            # Verify JSON output
            import json

            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["count"] == 1000

    @pytest.mark.asyncio
    async def test_execute_sql_query_empty_result(self):
        """Test handling empty result set."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film WHERE film_id = -1", tenant_id=1)

            # Verify empty JSON array
            import json

            data = json.loads(result)
            assert data == []

    @pytest.mark.asyncio
    async def test_execute_sql_query_single_row(self):
        """Test handling single row result."""
        mock_conn = AsyncMock()
        mock_rows = [{"film_id": 1, "title": "Test Film"}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query(
                "SELECT film_id, title FROM film WHERE film_id = 1", tenant_id=1
            )

            # Verify JSON output
            import json

            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["film_id"] == 1
            assert data[0]["title"] == "Test Film"

    @pytest.mark.asyncio
    async def test_execute_sql_query_multiple_rows(self):
        """Test handling multiple rows."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"film_id": 1, "title": "Film 1"},
            {"film_id": 2, "title": "Film 2"},
            {"film_id": 3, "title": "Film 3"},
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT film_id, title FROM film LIMIT 3", tenant_id=1)

            # Verify JSON output
            import json

            data = json.loads(result)
            assert len(data) == 3
            assert data[0]["film_id"] == 1
            assert data[2]["title"] == "Film 3"

    @pytest.mark.asyncio
    async def test_execute_sql_query_size_limit(self):
        """Test enforcing 1000 row limit."""
        mock_conn = AsyncMock()
        # Create 1001 rows
        mock_rows = [{"id": i} for i in range(1001)]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film", tenant_id=1)

            # Verify error message and truncated result
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
    async def test_execute_sql_query_forbidden_alter(self):
        """Test rejecting ALTER keyword."""
        result = await execute_sql_query("ALTER TABLE film ADD COLUMN test INT", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_grant(self):
        """Test rejecting GRANT keyword."""
        result = await execute_sql_query("GRANT SELECT ON film TO user", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_revoke(self):
        """Test rejecting REVOKE keyword."""
        result = await execute_sql_query("REVOKE SELECT ON film FROM user", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_truncate(self):
        """Test rejecting TRUNCATE keyword."""
        result = await execute_sql_query("TRUNCATE TABLE film", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_create(self):
        """Test rejecting CREATE keyword."""
        result = await execute_sql_query("CREATE TABLE test (id INT)", tenant_id=1)

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_case_insensitive(self):
        """Test case-insensitive security matching."""
        # Test lowercase
        result1 = await execute_sql_query("drop table film", tenant_id=1)
        assert "Error:" in result1

        # Test mixed case
        result2 = await execute_sql_query("DeLeTe FrOm film", tenant_id=1)
        assert "Error:" in result2

        # Test uppercase
        result3 = await execute_sql_query("INSERT INTO film", tenant_id=1)
        assert "Error:" in result3

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_word_boundaries(self):
        """Test word boundaries don't match keywords in table/column names."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": 1}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            # Should NOT be rejected (drop is part of table name)
            result = await execute_sql_query("SELECT * FROM drop_table", tenant_id=1)

            # Should succeed (not rejected)
            import json

            data = json.loads(result)
            assert len(data) == 1
            assert data[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_execute_sql_query_database_error(self):
        """Test handling PostgresError."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM nonexistent", tenant_id=1)

            # Verify error message
            assert "Database Error:" in result
            assert "Syntax error" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_general_error(self):
        """Test handling general exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film", tenant_id=1)

            # Verify error message
            assert "Execution Error:" in result
            assert "Unexpected error" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            await execute_sql_query("SELECT * FROM film", tenant_id=1)

    @pytest.mark.asyncio
    async def test_execute_sql_query_json_format(self):
        """Test JSON output format is correct."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"film_id": 1, "title": "Film 1", "release_year": 2020},
            {"film_id": 2, "title": "Film 2", "release_year": 2021},
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await execute_sql_query("SELECT * FROM film LIMIT 2", tenant_id=1)

            # Verify JSON is valid and formatted
            import json

            data = json.loads(result)
            assert isinstance(data, list)
            assert len(data) == 2
            assert data[0]["film_id"] == 1
            assert data[1]["title"] == "Film 2"


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

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await get_semantic_definitions(["High Value Customer"])

            # Verify connection was acquired (context manager called)
            mock_get.assert_called_once()

            # Verify query was executed with correct parameters
            mock_conn.fetch.assert_called_once()
            call_args = mock_conn.fetch.call_args
            assert "semantic_definitions" in call_args[0][0]
            assert "ANY(ARRAY[$1])" in call_args[0][0]
            assert call_args[0][1] == "High Value Customer"

            # Verify JSON output
            import json

            data = json.loads(result)
            assert "High Value Customer" in data
            assert (
                data["High Value Customer"]["definition"]
                == "Customer with lifetime payments > $150"
            )
            assert data["High Value Customer"]["sql_logic"] == "SUM(amount) > 150"

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_multiple_terms(self):
        """Test retrieving definitions for multiple terms."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "High Value Customer",
                "definition": "Customer with lifetime payments > $150",
                "sql_logic": "SUM(amount) > 150",
            },
            {
                "term_name": "Churned",
                "definition": "No rental activity in the last 30 days",
                "sql_logic": "last_rental_date < NOW() - INTERVAL '30 days'",
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await get_semantic_definitions(["High Value Customer", "Churned"])

            # Verify query used multiple parameters
            call_args = mock_conn.fetch.call_args
            assert "ANY(ARRAY[$1,$2])" in call_args[0][0]
            assert call_args[0][1] == "High Value Customer"
            assert call_args[0][2] == "Churned"

            # Verify JSON output
            import json

            data = json.loads(result)
            assert len(data) == 2
            assert "High Value Customer" in data
            assert "Churned" in data
            assert data["Churned"]["definition"] == "No rental activity in the last 30 days"

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_empty_list(self):
        """Test handling empty terms list."""
        result = await get_semantic_definitions([])

        # Should return empty JSON object without querying database
        import json

        data = json.loads(result)
        assert data == {}

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_term_not_found(self):
        """Test handling term not found in database."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # Empty result

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await get_semantic_definitions(["Nonexistent Term"])

            # Verify empty JSON object
            import json

            data = json.loads(result)
            assert data == {}

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_partial_match(self):
        """Test partial match where some terms found, some not."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "High Value Customer",
                "definition": "Customer with lifetime payments > $150",
                "sql_logic": "SUM(amount) > 150",
            }
            # "Churned" not in results
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await get_semantic_definitions(
                ["High Value Customer", "Churned", "Nonexistent"]
            )

            # Verify only found term is in result
            import json

            data = json.loads(result)
            assert len(data) == 1
            assert "High Value Customer" in data
            assert "Churned" not in data
            assert "Nonexistent" not in data

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_connection_error(self):
        """Test handling connection acquisition errors."""
        # Setup async context manager mock
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
            mock_get.side_effect = RuntimeError("Database pool not initialized")

            with pytest.raises(RuntimeError) as exc_info:
                await get_semantic_definitions(["High Value Customer"])

            assert "Database pool not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_query_error(self):
        """Test handling SQL query errors."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            with pytest.raises(asyncpg.PostgresError):
                await get_semantic_definitions(["High Value Customer"])

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            with pytest.raises(Exception):
                await get_semantic_definitions(["High Value Customer"])

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_json_format(self):
        """Test JSON output format is correct."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "High Value Customer",
                "definition": "Customer with lifetime payments > $150",
                "sql_logic": "SUM(amount) > 150",
            },
            {
                "term_name": "Gross Revenue",
                "definition": "Total sum of all payments",
                "sql_logic": "SUM(amount) FROM payment",
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            result = await get_semantic_definitions(["High Value Customer", "Gross Revenue"])

            # Verify JSON is valid and formatted
            import json

            data = json.loads(result)
            assert isinstance(data, dict)
            assert len(data) == 2
            assert "High Value Customer" in data
            assert "Gross Revenue" in data
            assert isinstance(data["High Value Customer"], dict)
            assert "definition" in data["High Value Customer"]
            assert "sql_logic" in data["High Value Customer"]

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_parameterized_query(self):
        """Test that query uses parameterized placeholders correctly."""
        mock_conn = AsyncMock()
        mock_rows = [
            {
                "term_name": "Term1",
                "definition": "Definition 1",
                "sql_logic": "SQL 1",
            },
            {
                "term_name": "Term2",
                "definition": "Definition 2",
                "sql_logic": "SQL 2",
            },
            {
                "term_name": "Term3",
                "definition": "Definition 3",
                "sql_logic": "SQL 3",
            },
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        # Setup async context manager mock
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):

            terms = ["Term1", "Term2", "Term3"]
            result = await get_semantic_definitions(terms)

            # Verify parameterized query construction
            call_args = mock_conn.fetch.call_args
            query = call_args[0][0]
            assert "ANY(ARRAY[$1,$2,$3])" in query
            assert call_args[0][1] == "Term1"
            assert call_args[0][2] == "Term2"
            assert call_args[0][3] == "Term3"

            # Verify all terms are in result
            import json

            data = json.loads(result)
            assert len(data) == 3
            assert "Term1" in data
            assert "Term2" in data
            assert "Term3" in data


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
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.RagEngine.embed_text", return_value=[0.1] * 384):
            with patch(
                "mcp_server.tools.legacy.search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch(
                "mcp_server.config.database.Database.get_schema_introspector"
            ) as mock_intro:
                mock_col = ColumnDef(name="id", data_type="int", is_nullable=False)
                mock_table_def = MagicMock()
                mock_table_def.columns = [mock_col]
                mock_table_def.foreign_keys = []
                mock_intro.return_value.get_table_def = AsyncMock(return_value=mock_table_def)
                with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
                    mock_search.return_value = mock_results

                    result = await search_relevant_tables("customer payment transactions", limit=5)

                    # Verify embedding was generated
                    from mcp_server.rag import RagEngine

                    RagEngine.embed_text.assert_called_once_with("customer payment transactions")

                    # Verify search was called
                    mock_search.assert_called_once()
                    call_args = mock_search.call_args
                    assert call_args[0][0] == [0.1] * 384  # embedding (positional)
                    assert call_args[1]["limit"] == 5  # limit (keyword)

                    # Verify JSON output
                    import json

                    data = json.loads(result)
                    assert len(data) == 2
                    assert data[0]["table_name"] == "payment"
                    assert data[1]["table_name"] == "customer"
                    assert data[0]["similarity"] == 0.9
                    assert data[1]["similarity"] == 0.8

    @pytest.mark.asyncio
    async def test_search_relevant_tables_empty_result(self):
        """Test empty results handling."""
        mock_conn = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.RagEngine.embed_text", return_value=[0.1] * 384):
            with patch(
                "mcp_server.tools.legacy.search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch(
                "mcp_server.config.database.Database.get_schema_introspector"
            ) as mock_intro:
                mock_intro.return_value.get_table_def.return_value.columns = []
                mock_intro.return_value.get_table_def.return_value.foreign_keys = []
                with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
                    mock_search.return_value = []

                    result = await search_relevant_tables("nonexistent query", limit=5)

                    import json

                    assert json.loads(result) == []

                    mock_search.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_relevant_tables_limit(self):
        """Test limit parameter."""
        mock_results = [
            {"table_name": f"table_{i}", "schema_text": f"text_{i}", "distance": float(i) * 0.1}
            for i in range(3)
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.RagEngine.embed_text", return_value=[0.1] * 384):
            with patch(
                "mcp_server.tools.legacy.search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch(
                "mcp_server.config.database.Database.get_schema_introspector"
            ) as mock_intro:
                mock_col = ColumnDef(name="id", data_type="int", is_nullable=False)
                mock_table_def = MagicMock()
                mock_table_def.columns = [mock_col]
                mock_table_def.foreign_keys = []
                mock_intro.return_value.get_table_def = AsyncMock(return_value=mock_table_def)
                with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
                    mock_search.return_value = mock_results

                    result = await search_relevant_tables("test query", limit=3)

                    # Verify limit was passed
                    call_args = mock_search.call_args
                    assert call_args[1]["limit"] == 3

                    import json

                    data = json.loads(result)
                    assert len(data) == 3
                    assert data[0]["table_name"] == "table_0"
                    assert data[1]["table_name"] == "table_1"
                    assert data[2]["table_name"] == "table_2"

    @pytest.mark.asyncio
    async def test_search_relevant_tables_markdown_formatting(self):
        """Test markdown formatting of results."""
        mock_results = [
            {
                "table_name": "payment",
                "schema_text": "Table: payment. Columns: payment_id, amount",
                "distance": 0.15,
            },
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.RagEngine.embed_text", return_value=[0.1] * 384):
            with patch(
                "mcp_server.tools.legacy.search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch(
                "mcp_server.config.database.Database.get_schema_introspector"
            ) as mock_intro:
                mock_col = ColumnDef(name="id", data_type="int", is_nullable=False)
                mock_table_def = MagicMock()
                mock_table_def.columns = [mock_col]
                mock_table_def.foreign_keys = []
                mock_intro.return_value.get_table_def = AsyncMock(return_value=mock_table_def)
                with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
                    mock_search.return_value = mock_results

                    result = await search_relevant_tables("payment query", limit=5)

                    # Verify JSON structure
                    import json

                    data = json.loads(result)
                    assert len(data) == 1
                    assert data[0]["table_name"] == "payment"
                    assert data[0]["description"] == "Table: payment. Columns: payment_id, amount"
                    assert data[0]["similarity"] == 0.85

    @pytest.mark.asyncio
    async def test_search_relevant_tables_similarity_calculation(self):
        """Test similarity score calculation (1 - distance)."""
        mock_results = [
            {"table_name": "table1", "schema_text": "text1", "distance": 0.0},  # Perfect match
            {"table_name": "table2", "schema_text": "text2", "distance": 0.5},  # 50% similar
            {"table_name": "table3", "schema_text": "text3", "distance": 1.0},  # No similarity
        ]

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_conn)

        with patch("mcp_server.tools.legacy.RagEngine.embed_text", return_value=[0.1] * 384):
            with patch(
                "mcp_server.tools.legacy.search_similar_tables", new_callable=AsyncMock
            ) as mock_search, patch(
                "mcp_server.config.database.Database.get_schema_introspector"
            ) as mock_intro:
                mock_col = ColumnDef(name="id", data_type="int", is_nullable=False)
                mock_table_def = MagicMock()
                mock_table_def.columns = [mock_col]
                mock_table_def.foreign_keys = []
                mock_intro.return_value.get_table_def = AsyncMock(return_value=mock_table_def)
                with patch("mcp_server.tools.legacy.Database.get_connection", mock_get):
                    mock_search.return_value = mock_results

                    result = await search_relevant_tables("test", limit=5)

                    import json

                    data = json.loads(result)
                    assert data[0]["similarity"] == 1.0
                    assert data[1]["similarity"] == 0.5
                    assert data[2]["similarity"] == 0.0

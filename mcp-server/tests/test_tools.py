"""Unit tests for MCP tool functions."""

from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from src.tools import execute_sql_query, get_semantic_definitions, get_table_schema, list_tables


class TestListTables:
    """Unit tests for list_tables function."""

    @pytest.mark.asyncio
    async def test_list_tables_all_tables(self):
        """Test listing all tables when no search_term is provided."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"table_name": "actor"},
            {"table_name": "film"},
            {"table_name": "payment"},
            {"table_name": "rental"},
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await list_tables()

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify query was executed
                mock_conn.fetch.assert_called_once()
                call_args = mock_conn.fetch.call_args[0]
                assert "SELECT table_name" in call_args[0]
                assert "table_schema = 'public'" in call_args[0]
                assert len(call_args) == 1  # No args when no search_term

                # Verify JSON output
                import json

                tables = json.loads(result)
                assert len(tables) == 4
                assert "actor" in tables
                assert "film" in tables
                assert "payment" in tables
                assert "rental" in tables

    @pytest.mark.asyncio
    async def test_list_tables_with_search_term(self):
        """Test filtering tables with search_term."""
        mock_conn = AsyncMock()
        mock_rows = [{"table_name": "payment"}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await list_tables("pay")

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify query included ILIKE filter
                mock_conn.fetch.assert_called_once()
                call_args = mock_conn.fetch.call_args[0]
                assert "ILIKE $1" in call_args[0]
                assert call_args[1] == "%pay%"

                # Verify filtered result
                import json

                tables = json.loads(result)
                assert len(tables) == 1
                assert "payment" in tables

    @pytest.mark.asyncio
    async def test_list_tables_empty_result(self):
        """Test handling empty result set."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await list_tables("nonexistent")

                # Verify connection was still released
                mock_release.assert_called_once_with(mock_conn)

                # Verify empty JSON array
                import json

                tables = json.loads(result)
                assert tables == []

    @pytest.mark.asyncio
    async def test_list_tables_connection_error(self):
        """Test handling connection acquisition errors."""
        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = RuntimeError("Database pool not initialized")

            with pytest.raises(RuntimeError) as exc_info:
                await list_tables()

            assert "Database pool not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_list_tables_query_error(self):
        """Test handling SQL query errors."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(asyncpg.PostgresError):
                    await list_tables()

                # Verify connection was still released even on error
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_list_tables_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(Exception):
                    await list_tables()

                # Verify connection was released in finally block
                mock_release.assert_called_once_with(mock_conn)


class TestGetTableSchema:
    """Unit tests for get_table_schema function."""

    @pytest.mark.asyncio
    async def test_get_table_schema_single_table(self):
        """Test retrieving schema for a single table."""
        mock_conn = AsyncMock()
        mock_cols = [
            {"column_name": "film_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "title", "data_type": "character varying", "is_nullable": "NO"},
        ]
        mock_fks = []

        async def mock_fetch(query, *args):
            if "information_schema.columns" in query:
                return mock_cols
            elif "FOREIGN KEY" in query:
                return mock_fks
            return []

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["film"])

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

                # Verify Markdown output
                assert "### Table: `film`" in result
                assert "| Column | Type | Nullable |" in result
                assert "| `film_id` | integer | NO |" in result
                assert "| `title` | character varying | NO |" in result

    @pytest.mark.asyncio
    async def test_get_table_schema_multiple_tables(self):
        """Test retrieving schema for multiple tables."""
        mock_conn = AsyncMock()

        async def mock_fetch(query, *args):
            if "information_schema.columns" in query:
                if "film" in query or args and args[0] == "film":
                    return [
                        {"column_name": "film_id", "data_type": "integer", "is_nullable": "NO"},
                    ]
                elif "actor" in query or (args and args[0] == "actor"):
                    return [
                        {"column_name": "actor_id", "data_type": "integer", "is_nullable": "NO"},
                    ]
            elif "FOREIGN KEY" in query:
                return []
            return []

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["film", "actor"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

                # Verify both tables are in output
                assert "### Table: `film`" in result
                assert "### Table: `actor`" in result
                assert "| `film_id`" in result
                assert "| `actor_id`" in result

    @pytest.mark.asyncio
    async def test_get_table_schema_with_foreign_keys(self):
        """Test schema retrieval includes foreign key information."""
        mock_conn = AsyncMock()
        mock_cols = [
            {"column_name": "film_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "language_id", "data_type": "integer", "is_nullable": "NO"},
        ]
        mock_fks = [
            {
                "column_name": "language_id",
                "foreign_table_name": "language",
                "foreign_column_name": "language_id",
            }
        ]

        async def mock_fetch(query, *args):
            if "information_schema.columns" in query:
                return mock_cols
            elif "FOREIGN KEY" in query:
                return mock_fks
            return []

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["film"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

                # Verify foreign keys section
                assert "**Foreign Keys:**" in result
                assert "`language_id` → `language.language_id`" in result

    @pytest.mark.asyncio
    async def test_get_table_schema_without_foreign_keys(self):
        """Test schema retrieval for table without foreign keys."""
        mock_conn = AsyncMock()
        mock_cols = [
            {"column_name": "actor_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "first_name", "data_type": "character varying", "is_nullable": "NO"},
        ]
        mock_fks = []

        async def mock_fetch(query, *args):
            if "information_schema.columns" in query:
                return mock_cols
            elif "FOREIGN KEY" in query:
                return mock_fks
            return []

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["actor"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

                # Verify no foreign keys section
                assert "**Foreign Keys:**" not in result
                assert "### Table: `actor`" in result

    @pytest.mark.asyncio
    async def test_get_table_schema_table_not_found(self):
        """Test handling non-existent tables."""
        mock_conn = AsyncMock()

        async def mock_fetch(query, *args):
            return []  # Empty result for table not found

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["nonexistent"])

                # Verify "Not Found" message
                assert "### Table: nonexistent (Not Found)" in result
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_get_table_schema_empty_list(self):
        """Test handling empty table_names list."""
        mock_conn = AsyncMock()

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema([])

                # Verify empty output
                assert result == ""
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_get_table_schema_connection_error(self):
        """Test handling connection acquisition errors."""
        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = RuntimeError("Database pool not initialized")

            with pytest.raises(RuntimeError) as exc_info:
                await get_table_schema(["film"])

            assert "Database pool not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_table_schema_query_error(self):
        """Test handling SQL query errors."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(asyncpg.PostgresError):
                    await get_table_schema(["film"])

                # Verify connection was still released even on error
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_get_table_schema_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(Exception):
                    await get_table_schema(["film"])

                # Verify connection was released in finally block
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_get_table_schema_markdown_format(self):
        """Test Markdown output format is correct."""
        mock_conn = AsyncMock()
        mock_cols = [
            {"column_name": "film_id", "data_type": "integer", "is_nullable": "NO"},
            {"column_name": "title", "data_type": "character varying", "is_nullable": "YES"},
        ]
        mock_fks = [
            {
                "column_name": "language_id",
                "foreign_table_name": "language",
                "foreign_column_name": "language_id",
            }
        ]

        async def mock_fetch(query, *args):
            if "information_schema.columns" in query:
                return mock_cols
            elif "FOREIGN KEY" in query:
                return mock_fks
            return []

        mock_conn.fetch = AsyncMock(side_effect=mock_fetch)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_table_schema(["film"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

                # Verify Markdown structure
                lines = result.split("\n")
                assert lines[0] == "### Table: `film`"
                assert lines[1] == ""
                assert "| Column | Type | Nullable |" in lines
                assert "|---|---|---|" in lines
                assert "| `film_id` | integer | NO |" in result
                assert "| `title` | character varying | YES |" in result
                assert "**Foreign Keys:**" in result
                assert "- `language_id` → `language.language_id`" in result


class TestExecuteSqlQuery:
    """Unit tests for execute_sql_query function."""

    @pytest.mark.asyncio
    async def test_execute_sql_query_valid_select(self):
        """Test executing a valid SELECT query."""
        mock_conn = AsyncMock()
        mock_rows = [{"count": 1000}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT COUNT(*) as count FROM film")

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT * FROM film WHERE film_id = -1")

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query(
                    "SELECT film_id, title FROM film WHERE film_id = 1"
                )

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT film_id, title FROM film LIMIT 3")

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT * FROM film")

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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
        result = await execute_sql_query("DROP TABLE film")

        assert "Error:" in result
        assert "forbidden keyword" in result
        assert "DROP" in result or "drop" in result.lower()

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_delete(self):
        """Test rejecting DELETE keyword."""
        result = await execute_sql_query("DELETE FROM film WHERE film_id = 1")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_insert(self):
        """Test rejecting INSERT keyword."""
        result = await execute_sql_query("INSERT INTO film VALUES (1, 'Test')")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_update(self):
        """Test rejecting UPDATE keyword."""
        result = await execute_sql_query("UPDATE film SET title = 'Test'")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_alter(self):
        """Test rejecting ALTER keyword."""
        result = await execute_sql_query("ALTER TABLE film ADD COLUMN test INT")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_grant(self):
        """Test rejecting GRANT keyword."""
        result = await execute_sql_query("GRANT SELECT ON film TO user")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_revoke(self):
        """Test rejecting REVOKE keyword."""
        result = await execute_sql_query("REVOKE SELECT ON film FROM user")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_truncate(self):
        """Test rejecting TRUNCATE keyword."""
        result = await execute_sql_query("TRUNCATE TABLE film")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_forbidden_create(self):
        """Test rejecting CREATE keyword."""
        result = await execute_sql_query("CREATE TABLE test (id INT)")

        assert "Error:" in result
        assert "forbidden keyword" in result

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_case_insensitive(self):
        """Test case-insensitive security matching."""
        # Test lowercase
        result1 = await execute_sql_query("drop table film")
        assert "Error:" in result1

        # Test mixed case
        result2 = await execute_sql_query("DeLeTe FrOm film")
        assert "Error:" in result2

        # Test uppercase
        result3 = await execute_sql_query("INSERT INTO film")
        assert "Error:" in result3

    @pytest.mark.asyncio
    async def test_execute_sql_query_security_word_boundaries(self):
        """Test word boundaries don't match keywords in table/column names."""
        mock_conn = AsyncMock()
        mock_rows = [{"id": 1}]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                # Should NOT be rejected (drop is part of table name)
                result = await execute_sql_query("SELECT * FROM drop_table")

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT * FROM nonexistent")

                # Verify error message
                assert "Database Error:" in result
                assert "Syntax error" in result

                # Verify connection was still released
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_execute_sql_query_general_error(self):
        """Test handling general exceptions."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT * FROM film")

                # Verify error message
                assert "Execution Error:" in result
                assert "Unexpected error" in result

                # Verify connection was still released
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_execute_sql_query_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                await execute_sql_query("SELECT * FROM film")

                # Verify connection was released in finally block
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_execute_sql_query_json_format(self):
        """Test JSON output format is correct."""
        mock_conn = AsyncMock()
        mock_rows = [
            {"film_id": 1, "title": "Film 1", "release_year": 2020},
            {"film_id": 2, "title": "Film 2", "release_year": 2021},
        ]
        mock_conn.fetch = AsyncMock(return_value=mock_rows)

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await execute_sql_query("SELECT * FROM film LIMIT 2")

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

                # Verify JSON is valid and formatted
                import json

                data = json.loads(result)
                assert isinstance(data, list)
                assert len(data) == 2
                assert data[0]["film_id"] == 1
                assert data[1]["title"] == "Film 2"

                # Verify it's formatted (indent=2)
                assert "\n" in result


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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_semantic_definitions(["High Value Customer"])

                # Verify connection was acquired and released
                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_semantic_definitions(["High Value Customer", "Churned"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_semantic_definitions(["Nonexistent Term"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_semantic_definitions(
                    ["High Value Customer", "Churned", "Nonexistent"]
                )

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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
        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = RuntimeError("Database pool not initialized")

            with pytest.raises(RuntimeError) as exc_info:
                await get_semantic_definitions(["High Value Customer"])

            assert "Database pool not initialized" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_query_error(self):
        """Test handling SQL query errors."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=asyncpg.PostgresError("Syntax error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(asyncpg.PostgresError):
                    await get_semantic_definitions(["High Value Customer"])

                # Verify connection was still released even on error
                mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_get_semantic_definitions_always_releases_connection(self):
        """Test that connection is always released even if an exception occurs."""
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(side_effect=Exception("Unexpected error"))

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                with pytest.raises(Exception):
                    await get_semantic_definitions(["High Value Customer"])

                # Verify connection was released in finally block
                mock_release.assert_called_once_with(mock_conn)

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                result = await get_semantic_definitions(["High Value Customer", "Gross Revenue"])

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

                # Verify it's formatted (indent=2)
                assert "\n" in result

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

        with patch("src.tools.Database.get_connection", new_callable=AsyncMock) as mock_get:
            with patch(
                "src.tools.Database.release_connection", new_callable=AsyncMock
            ) as mock_release:
                mock_get.return_value = mock_conn

                terms = ["Term1", "Term2", "Term3"]
                result = await get_semantic_definitions(terms)

                # Verify connection was released
                mock_release.assert_called_once_with(mock_conn)

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

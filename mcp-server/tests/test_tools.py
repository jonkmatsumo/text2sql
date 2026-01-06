"""Unit tests for MCP tool functions."""

from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from src.tools import get_table_schema, list_tables


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

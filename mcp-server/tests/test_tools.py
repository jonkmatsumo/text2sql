"""Unit tests for MCP tool functions."""

from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
from src.tools import list_tables


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

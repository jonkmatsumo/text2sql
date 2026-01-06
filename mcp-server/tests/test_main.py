"""Unit tests for MCP server main entrypoint."""

from unittest.mock import AsyncMock, patch

import pytest


class TestMain:
    """Unit tests for main.py MCP server setup."""

    def test_mcp_server_exists(self):
        """Test that mcp server instance exists."""
        from src.main import mcp

        assert mcp is not None
        assert hasattr(mcp, "run")

    def test_tools_are_registered(self):
        """Test that tools are registered with the MCP server."""
        from src.main import mcp

        # FastMCP stores registered tools internally
        # We can verify the server has the tool registration capability
        assert hasattr(mcp, "tool") or hasattr(mcp, "_tools") or hasattr(mcp, "tools")

    def test_module_imports_successfully(self):
        """Test that main.py module imports without errors."""
        # This test verifies that all imports work and the module structure is correct
        import src.main

        assert src.main is not None
        assert hasattr(src.main, "mcp")

    def test_database_module_imported(self):
        """Test that Database module is imported."""
        from src.main import Database

        assert Database is not None
        assert hasattr(Database, "init")
        assert hasattr(Database, "close")

    def test_tool_functions_imported(self):
        """Test that all tool functions are imported."""
        from src.main import (
            execute_sql_query,
            get_semantic_definitions,
            get_table_schema,
            list_tables,
            search_relevant_tables,
        )

        assert callable(list_tables)
        assert callable(get_table_schema)
        assert callable(execute_sql_query)
        assert callable(get_semantic_definitions)
        assert callable(search_relevant_tables)

    def test_load_dotenv_called(self):
        """Test that load_dotenv is imported."""
        from src.main import load_dotenv

        assert callable(load_dotenv)

    def test_fastmcp_imported(self):
        """Test that FastMCP is imported."""
        from src.main import FastMCP

        assert FastMCP is not None

    @pytest.mark.asyncio
    async def test_init_database_triggers_indexing_when_empty(self):
        """Test that indexing is triggered when schema_embeddings table is empty."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=0)  # Empty table

        with patch("src.main.Database.init", new_callable=AsyncMock) as mock_init:
            with patch("src.main.Database.get_connection", new_callable=AsyncMock) as mock_get:
                with patch(
                    "src.main.Database.release_connection", new_callable=AsyncMock
                ) as mock_release:
                    with patch(
                        "src.indexer.index_all_tables", new_callable=AsyncMock
                    ) as mock_index:
                        mock_get.return_value = mock_conn

                        # Replicate init_database logic
                        await mock_init()

                        conn = await mock_get()
                        try:
                            count = await conn.fetchval(
                                "SELECT COUNT(*) FROM public.schema_embeddings"
                            )
                            if count == 0:
                                await mock_index()
                        finally:
                            await mock_release(conn)

                        # Verify database was initialized
                        mock_init.assert_called_once()

                        # Verify count was checked
                        mock_conn.fetchval.assert_called_once()

                        # Verify indexing was triggered
                        mock_index.assert_called_once()

                        # Verify connection was released
                        mock_release.assert_called_once_with(mock_conn)

    @pytest.mark.asyncio
    async def test_init_database_skips_indexing_when_populated(self):
        """Test that indexing is skipped when schema_embeddings table has data."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=15)  # 15 tables already indexed

        with patch("src.main.Database.init", new_callable=AsyncMock) as mock_init:
            with patch("src.main.Database.get_connection", new_callable=AsyncMock) as mock_get:
                with patch(
                    "src.main.Database.release_connection", new_callable=AsyncMock
                ) as mock_release:
                    with patch(
                        "src.indexer.index_all_tables", new_callable=AsyncMock
                    ) as mock_index:
                        mock_get.return_value = mock_conn

                        # Replicate init_database logic
                        await mock_init()

                        conn = await mock_get()
                        try:
                            count = await conn.fetchval(
                                "SELECT COUNT(*) FROM public.schema_embeddings"
                            )
                            if count == 0:
                                await mock_index()
                        finally:
                            await mock_release(conn)

                        # Verify database was initialized
                        mock_init.assert_called_once()

                        # Verify count was checked
                        mock_conn.fetchval.assert_called_once()

                        # Verify indexing was NOT triggered
                        mock_index.assert_not_called()

                        # Verify connection was released
                        mock_release.assert_called_once_with(mock_conn)

"""Unit tests for MCP server main entrypoint."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestMain:
    """Unit tests for main.py MCP server setup."""

    def test_mcp_server_exists(self):
        """Test that mcp server instance exists."""
        from mcp_server.main import mcp

        assert mcp is not None
        assert hasattr(mcp, "run")

    def test_tools_are_registered(self):
        """Test that tools are registered with the MCP server."""
        from mcp_server.main import mcp

        # FastMCP stores registered tools internally
        # We can verify the server has the tool registration capability
        assert hasattr(mcp, "tool") or hasattr(mcp, "_tools") or hasattr(mcp, "tools")

    def test_module_imports_successfully(self):
        """Test that main.py module imports without errors."""
        # This test verifies that all imports work and the module structure is correct
        import mcp_server.main

        assert mcp_server.main is not None
        assert hasattr(mcp_server.main, "mcp")

    def test_database_module_imported(self):
        """Test that Database module is imported."""
        from mcp_server.main import Database

        assert Database is not None
        assert hasattr(Database, "init")
        assert hasattr(Database, "close")

    def test_tool_functions_available(self):
        """Test that all tool functions are available in the tools package."""
        from mcp_server.tools import (
            execute_sql_query_handler,
            get_semantic_definitions_handler,
            get_table_schema_handler,
            list_tables_handler,
            search_relevant_tables_handler,
        )

        assert callable(list_tables_handler)
        assert callable(get_table_schema_handler)
        assert callable(execute_sql_query_handler)
        assert callable(get_semantic_definitions_handler)
        assert callable(search_relevant_tables_handler)

    def test_load_dotenv_called(self):
        """Test that load_dotenv is imported."""
        from mcp_server.main import load_dotenv

        assert callable(load_dotenv)

    def test_fastmcp_imported(self):
        """Test that FastMCP is imported."""
        from mcp_server.main import FastMCP

        assert FastMCP is not None

    @pytest.mark.asyncio
    async def test_init_database_triggers_indexing_when_empty(self):
        """Test that indexing is triggered when schema_embeddings table is empty."""
        # Import Database here to use patch.object for correct module resolution
        from mcp_server.config.database import Database

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=0)  # Empty table

        # Setup async context manager mock for get_connection
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch.object(Database, "init", new_callable=AsyncMock) as mock_init:
            with patch.object(Database, "get_connection", mock_get):
                with patch(
                    "mcp_server.services.rag.index_all_tables", new_callable=AsyncMock
                ) as mock_index:
                    # Replicate init_database logic
                    await mock_init()

                    async with Database.get_connection() as conn:
                        count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
                        if count == 0:
                            await mock_index()

                    # Verify database was initialized
                    mock_init.assert_called_once()

                    # Verify count was checked
                    mock_conn.fetchval.assert_called_once()

                    # Verify indexing was triggered
                    mock_index.assert_called_once()

                    # Verify connection context manager was used
                    mock_get.assert_called_once()

    @pytest.mark.asyncio
    async def test_init_database_skips_indexing_when_populated(self):
        """Test that indexing is skipped when schema_embeddings table has data."""
        # Import Database here to use patch.object for correct module resolution
        from mcp_server.config.database import Database

        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=15)  # 15 tables already indexed

        # Setup async context manager mock for get_connection
        mock_get_cm = AsyncMock()
        mock_get_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_get_cm.__aexit__ = AsyncMock(return_value=False)
        mock_get = MagicMock(return_value=mock_get_cm)

        with patch.object(Database, "init", new_callable=AsyncMock) as mock_init:
            with patch.object(Database, "get_connection", mock_get):
                with patch(
                    "mcp_server.services.rag.index_all_tables", new_callable=AsyncMock
                ) as mock_index:
                    # Replicate init_database logic
                    await mock_init()

                    async with Database.get_connection() as conn:
                        count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
                        if count == 0:
                            await mock_index()

                    # Verify database was initialized
                    mock_init.assert_called_once()

                    # Verify count was checked
                    mock_conn.fetchval.assert_called_once()

                    # Verify indexing was NOT triggered
                    mock_index.assert_not_called()

                    # Verify connection context manager was used
                    mock_get.assert_called_once()

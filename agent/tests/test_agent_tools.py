"""Unit tests for MCP server tool integration.

NOTE:
Renamed from test_tools.py to test_agent_tools.py to avoid pytest module
name collisions with mcp-server/tests/test_tools.py when running tests
from the repo root.
"""

import os
import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure we can import agent_core.tools even if dependencies are missing
# We mock langchain_mcp_adapters.client before importing the module under test
if "langchain_mcp_adapters" not in sys.modules:
    mock_pkg = ModuleType("langchain_mcp_adapters")
    mock_client_mod = ModuleType("langchain_mcp_adapters.client")
    mock_client_mod.MultiServerMCPClient = MagicMock()
    mock_pkg.client = mock_client_mod
    sys.modules["langchain_mcp_adapters"] = mock_pkg
    sys.modules["langchain_mcp_adapters.client"] = mock_client_mod

from agent_core.tools import get_mcp_tools


class TestGetMcpTools:
    """Unit tests for get_mcp_tools function."""

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Clean environment variables and ensure modules are mocked."""
        # Setup mocks for langchain_mcp_adapters if not present or overwritten
        mock_pkg = ModuleType("langchain_mcp_adapters")
        mock_client_mod = ModuleType("langchain_mcp_adapters.client")
        mock_client_mod.MultiServerMCPClient = MagicMock()
        mock_pkg.client = mock_client_mod

        # We use patch.dict to safely modify sys.modules for the duration of the test
        with patch.dict(
            sys.modules,
            {
                "langchain_mcp_adapters": mock_pkg,
                "langchain_mcp_adapters.client": mock_client_mod,
            },
        ):
            with patch.dict(os.environ, {}, clear=True):
                yield

    @pytest.mark.asyncio
    async def test_get_mcp_tools_success(self):
        """Test successful connection to MCP server and tool retrieval."""
        # Patch the class where it is imported FROM, since it is imported inside the function
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_tool1 = MagicMock()
            mock_tool1.name = "list_tables"
            mock_tool2 = MagicMock()
            mock_tool2.name = "get_table_schema"
            mock_tool3 = MagicMock()
            mock_tool3.name = "execute_sql_query"
            mock_tool4 = MagicMock()
            mock_tool4.name = "get_semantic_definitions"
            mock_tool5 = MagicMock()
            mock_tool5.name = "search_relevant_tables"

            mock_client.get_tools.return_value = [
                mock_tool1,
                mock_tool2,
                mock_tool3,
                mock_tool4,
                mock_tool5,
            ]
            mock_client_class.return_value = mock_client

            # Set environment variable
            os.environ["MCP_SERVER_URL"] = "http://localhost:8000/mcp"

            result = await get_mcp_tools()

            # Verify MultiServerMCPClient was created with correct config
            mock_client_class.assert_called_once_with(
                {
                    "data-layer": {
                        "url": "http://localhost:8000/mcp",
                        "transport": "sse",
                    }
                }
            )

            # Verify get_tools was called
            mock_client.get_tools.assert_called_once()

            # Verify all 5 expected tools are present
            assert len(result) == 5
            tool_names = [tool.name for tool in result]
            assert "list_tables" in tool_names
            assert "get_table_schema" in tool_names
            assert "execute_sql_query" in tool_names
            assert "get_semantic_definitions" in tool_names
            assert "search_relevant_tables" in tool_names

    @pytest.mark.asyncio
    async def test_get_mcp_tools_default_url(self):
        """Test that default URL is used when MCP_SERVER_URL is not set."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_tools.return_value = []
            mock_client_class.return_value = mock_client

            # Don't set MCP_SERVER_URL
            result = await get_mcp_tools()

            # Verify default URL was used
            mock_client_class.assert_called_once_with(
                {
                    "data-layer": {
                        "url": "http://localhost:8000/messages",
                        "transport": "sse",
                    }
                }
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_custom_url(self):
        """Test with custom MCP_SERVER_URL."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_tools.return_value = []
            mock_client_class.return_value = mock_client

            # Set custom URL
            os.environ["MCP_SERVER_URL"] = "http://custom-host:9000/mcp"

            await get_mcp_tools()

            # Verify custom URL was used
            mock_client_class.assert_called_once_with(
                {
                    "data-layer": {
                        "url": "http://custom-host:9000/mcp",
                        "transport": "sse",
                    }
                }
            )

    @pytest.mark.asyncio
    async def test_get_mcp_tools_server_name(self):
        """Test that server name is 'data-layer'."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_tools.return_value = []
            mock_client_class.return_value = mock_client

            await get_mcp_tools()

            # Verify server name is 'data-layer'
            call_args = mock_client_class.call_args[0][0]
            assert "data-layer" in call_args
            assert call_args["data-layer"]["transport"] == "sse"

    @pytest.mark.asyncio
    async def test_get_mcp_tools_transport(self):
        """Test that transport is 'sse'."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_tools.return_value = []
            mock_client_class.return_value = mock_client

            await get_mcp_tools()

            # Verify transport is 'sse'
            call_args = mock_client_class.call_args[0][0]
            assert call_args["data-layer"]["transport"] == "sse"

    @pytest.mark.asyncio
    async def test_get_mcp_tools_connection_error(self):
        """Test error handling for connection failures."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            # Making get_tools raise an exception mimics a connection failure during tool fetching
            # Or simpler: constructor could fail, but client implies lazy connection.
            # Let's assume get_tools() does the I/O.
            mock_client.get_tools.side_effect = Exception("Connection failed")
            mock_client_class.return_value = mock_client

            # Verify exception is raised
            with pytest.raises(Exception, match="Connection failed"):
                await get_mcp_tools()

    @pytest.mark.asyncio
    async def test_get_mcp_tools_empty_result(self):
        """Test handling of empty tool list."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.get_tools.return_value = []
            mock_client_class.return_value = mock_client

            result = await get_mcp_tools()

            assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_partial_tools(self):
        """Test handling when only some tools are available."""
        with patch("langchain_mcp_adapters.client.MultiServerMCPClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_tool1 = MagicMock()
            mock_tool1.name = "list_tables"
            mock_tool2 = MagicMock()
            mock_tool2.name = "execute_sql_query"

            mock_client.get_tools.return_value = [mock_tool1, mock_tool2]
            mock_client_class.return_value = mock_client

            result = await get_mcp_tools()

            assert len(result) == 2
            tool_names = [tool.name for tool in result]
            assert "list_tables" in tool_names
            assert "execute_sql_query" in tool_names

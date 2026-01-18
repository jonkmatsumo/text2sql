"""Unit tests for MCP server tool integration.

Tests for get_mcp_tools() using MCPClient (official MCP SDK).

NOTE:
Renamed from test_tools.py to test_agent_tools.py to avoid pytest module
name collisions with mcp-server/tests/test_tools.py when running tests
from the repo root.
"""

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock the mcp SDK before importing agent_core modules that depend on it
if "mcp" not in sys.modules:
    mcp_mock = MagicMock()
    mcp_mock.ClientSession = MagicMock()
    mcp_mock.types = MagicMock()
    mcp_mock.types.TextContent = MagicMock()
    sys.modules["mcp"] = mcp_mock
    sys.modules["mcp.client"] = MagicMock()
    sys.modules["mcp.client.sse"] = MagicMock()
    sys.modules["mcp.client.streamable_http"] = MagicMock()

from agent_core.mcp.sdk_client import ToolInfo  # noqa: E402
from agent_core.tools import get_mcp_tools  # noqa: E402


def create_mock_tool_info(name: str, description: str = "") -> ToolInfo:
    """Create a mock ToolInfo for testing."""
    return ToolInfo(
        name=name,
        description=description or f"{name} tool description",
        input_schema={"type": "object", "properties": {}},
    )


class TestGetMcpTools:
    """Unit tests for get_mcp_tools function."""

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Clean environment variables for each test."""
        with patch.dict(os.environ, {}, clear=True):
            yield

    @pytest.mark.asyncio
    async def test_get_mcp_tools_success(self):
        """Test successful connection to MCP server and tool retrieval."""
        mock_tool_infos = [
            create_mock_tool_info("list_tables"),
            create_mock_tool_info("get_table_schema"),
            create_mock_tool_info("execute_sql_query"),
            create_mock_tool_info("get_semantic_definitions"),
            create_mock_tool_info("search_relevant_tables"),
        ]

        with patch("agent_core.tools.MCPClient") as mock_client_class:
            # Setup mock client
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm

            # Setup list_tools response
            mock_client.list_tools = AsyncMock(return_value=mock_tool_infos)

            # Set environment variable
            os.environ["MCP_SERVER_URL"] = "http://localhost:8000/mcp"

            result = await get_mcp_tools()

            # Verify MCPClient was created with correct config
            mock_client_class.assert_called_once_with(
                server_url="http://localhost:8000/mcp",
                transport="sse",
            )

            # Verify list_tools was called
            mock_client.list_tools.assert_called_once()

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
        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=[])

            # Don't set MCP_SERVER_URL
            result = await get_mcp_tools()

            # Verify default URL was used
            mock_client_class.assert_called_once_with(
                server_url="http://localhost:8000/messages",
                transport="sse",
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_custom_url(self):
        """Test with custom MCP_SERVER_URL."""
        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=[])

            # Set custom URL
            os.environ["MCP_SERVER_URL"] = "http://custom-host:9000/mcp"

            await get_mcp_tools()

            # Verify custom URL was used
            mock_client_class.assert_called_once_with(
                server_url="http://custom-host:9000/mcp",
                transport="sse",
            )

    @pytest.mark.asyncio
    async def test_get_mcp_tools_custom_transport(self):
        """Test with custom MCP_TRANSPORT."""
        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=[])

            # Set custom transport
            os.environ["MCP_TRANSPORT"] = "streamable-http"

            await get_mcp_tools()

            # Verify custom transport was used
            mock_client_class.assert_called_once_with(
                server_url="http://localhost:8000/messages",
                transport="streamable-http",
            )

    @pytest.mark.asyncio
    async def test_get_mcp_tools_connection_error(self):
        """Test error handling for connection failures."""
        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.side_effect = Exception("Connection failed")
            mock_client_class.return_value.connect.return_value = mock_connect_cm

            # Verify exception is raised
            with pytest.raises(Exception, match="Connection failed"):
                await get_mcp_tools()

    @pytest.mark.asyncio
    async def test_get_mcp_tools_empty_result(self):
        """Test handling of empty tool list."""
        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=[])

            result = await get_mcp_tools()

            assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_partial_tools(self):
        """Test handling when only some tools are available."""
        mock_tool_infos = [
            create_mock_tool_info("list_tables"),
            create_mock_tool_info("execute_sql_query"),
        ]

        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=mock_tool_infos)

            result = await get_mcp_tools()

            assert len(result) == 2
            tool_names = [tool.name for tool in result]
            assert "list_tables" in tool_names
            assert "execute_sql_query" in tool_names

    @pytest.mark.asyncio
    async def test_get_mcp_tools_wraps_with_telemetry(self):
        """Test that tools are wrapped with telemetry."""
        mock_tool_infos = [create_mock_tool_info("test_tool")]

        with patch("agent_core.tools.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.return_value = mock_client
            mock_connect_cm.__aexit__.return_value = None
            mock_client_class.return_value.connect.return_value = mock_connect_cm
            mock_client.list_tools = AsyncMock(return_value=mock_tool_infos)

            result = await get_mcp_tools()

            # Verify tool has required interface
            assert len(result) == 1
            tool = result[0]
            assert hasattr(tool, "name")
            assert hasattr(tool, "ainvoke")
            assert tool.name == "test_tool"

    @pytest.mark.asyncio
    async def test_tool_ainvoke_with_config_parameter(self):
        """Test that tool.ainvoke works with config parameter (LangGraph compat).

        LangGraph passes config dict to tools for tracing/callbacks. The wrapper
        must accept config=None or config=dict without raising.
        """
        mock_tool_infos = [create_mock_tool_info("test_tool")]

        # Patch telemetry at the import location (inside _wrap_tool)
        with patch("agent_core.telemetry.telemetry") as mock_telemetry:
            mock_span = MagicMock()
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=None)
            mock_telemetry.start_span.return_value = mock_span

            with patch("agent_core.tools.MCPClient") as mock_client_class:
                # Setup discovery mock
                mock_client = MagicMock()
                mock_connect_cm = AsyncMock()
                mock_connect_cm.__aenter__.return_value = mock_client
                mock_connect_cm.__aexit__.return_value = None
                mock_client_class.return_value.connect.return_value = mock_connect_cm
                mock_client.list_tools = AsyncMock(return_value=mock_tool_infos)

                # Setup invocation mock
                mock_client.call_tool = AsyncMock(return_value={"tables": ["film"]})

                tools = await get_mcp_tools()
                tool = tools[0]

                # Test with config=None (default)
                result1 = await tool.ainvoke({"query": "test"})
                assert result1 == {"tables": ["film"]}

                # Test with config=dict (LangGraph style)
                result2 = await tool.ainvoke(
                    {"query": "test2"}, config={"tags": ["test"], "callbacks": []}
                )
                assert result2 == {"tables": ["film"]}

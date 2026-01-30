"""Unit tests for MCP server tool integration.

Tests for get_mcp_tools() using MCPClient (official MCP SDK).

NOTE:
Renamed from test_tools.py to test_agent_tools.py to avoid pytest module
name collisions with mcp-server/tests/test_tools.py when running tests
from the repo root.
"""

import importlib.machinery
import importlib.util
import os
import sys
import types
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

# Mock the mcp SDK only if not installed, to allow import of agent modules
if importlib.util.find_spec("mcp") is None:

    def create_mock_module(name):
        """Create a mock module for testing."""
        mock = types.ModuleType(name)
        mock.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
        mock.__path__ = []
        sys.modules[name] = mock
        return mock

    mcp_mock = create_mock_module("mcp")
    mcp_mock.ClientSession = MagicMock()

    mcp_types = create_mock_module("mcp_server.types")
    mcp_types.TextContent = MagicMock()
    mcp_mock.types = mcp_types

    create_mock_module("mcp_server.client")
    create_mock_module("mcp_server.client.sse")
    create_mock_module("mcp_server.client.streamable_http")


from agent.mcp_client.sdk_client import ToolInfo  # noqa: E402
from agent.tools import get_mcp_tools  # noqa: E402


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

        with patch("agent.tools.MCPClient") as mock_client_class:
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
                headers=ANY,
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
        with patch("agent.tools.MCPClient") as mock_client_class:
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
                headers=ANY,
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_custom_url(self):
        """Test with custom MCP_SERVER_URL."""
        with patch("agent.tools.MCPClient") as mock_client_class:
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
                headers=ANY,
            )

    @pytest.mark.asyncio
    async def test_get_mcp_tools_custom_transport(self):
        """Test with custom MCP_TRANSPORT."""
        with patch("agent.tools.MCPClient") as mock_client_class:
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
                headers=ANY,
            )

    @pytest.mark.asyncio
    async def test_get_mcp_tools_connection_error(self):
        """Test error handling for connection failures."""
        with patch("agent.tools.MCPClient") as mock_client_class:
            mock_connect_cm = AsyncMock()
            mock_connect_cm.__aenter__.side_effect = Exception("Connection failed")
            mock_client_class.return_value.connect.return_value = mock_connect_cm

            # Verify exception is raised
            with pytest.raises(Exception, match="Connection failed"):
                await get_mcp_tools()

    @pytest.mark.asyncio
    async def test_get_mcp_tools_empty_result(self):
        """Test handling of empty tool list."""
        with patch("agent.tools.MCPClient") as mock_client_class:
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

        with patch("agent.tools.MCPClient") as mock_client_class:
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

        with patch("agent.tools.MCPClient") as mock_client_class:
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
    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Skipped in CI - requires complex mocking of MCP client internals",
    )
    async def test_tool_ainvoke_with_config_parameter(self):
        """Test that tool.ainvoke works with config parameter (LangGraph compat).

        LangGraph passes config dict to tools for tracing/callbacks. The wrapper
        must accept config=None or config=dict without raising.

        NOTE: This test is skipped in CI because it requires complex mocking of
        the MCP client internals including the resilient invoke function.
        """
        mock_tool_infos = [create_mock_tool_info("test_tool")]

        # Patch telemetry at the import location (inside _wrap_tool)
        with patch("agent.telemetry.telemetry") as mock_telemetry:
            mock_span = MagicMock()
            mock_span.__enter__ = MagicMock(return_value=mock_span)
            mock_span.__exit__ = MagicMock(return_value=None)
            mock_telemetry.start_span.return_value = mock_span

            with patch("agent.tools.MCPClient") as mock_client_class:
                # Setup discovery mock
                mock_client = MagicMock()
                mock_connect_cm = AsyncMock()
                mock_connect_cm.__aenter__.return_value = mock_client
                mock_connect_cm.__aexit__.return_value = None
                mock_client_class.return_value.connect.return_value = mock_connect_cm
                mock_client.list_tools = AsyncMock(return_value=mock_tool_infos)

                # Also patch the resilient invoke function that's used for tool calls
                # The invoke_fn now uses create_resilient_invoke_fn which creates a new manager
                async def mock_invoke(arguments):
                    return {"tables": ["film"]}

                with patch(
                    "agent.tools.create_resilient_invoke_fn",
                    return_value=mock_invoke,
                ):
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

"""Unit tests for MCP server tool integration.

NOTE:
Renamed from test_tools.py to test_agent_tools.py to avoid pytest module
name collisions with mcp-server/tests/test_tools.py when running tests
from the repo root.
"""

import importlib.util
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def isolated_agent_tools():
    """Load agent_core.tools from source to ensure isolation."""
    source_path = os.path.abspath("agent/src/agent_core/tools.py")
    if not os.path.exists(source_path):
        pytest.fail(f"Could not find agent_core.tools source at {source_path}")

    spec = importlib.util.spec_from_file_location("agent_core.tools_isolated", source_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGetMcpTools:
    """Unit tests for get_mcp_tools function."""

    @pytest.mark.asyncio
    async def test_get_mcp_tools_success(self, isolated_agent_tools):
        """Test successful connection to MCP server and tool retrieval."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        # Patch on the isolated module
        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    # Create mock client instance
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
                    mock_client.get_tools = AsyncMock(
                        return_value=[
                            mock_tool1,
                            mock_tool2,
                            mock_tool3,
                            mock_tool4,
                            mock_tool5,
                        ]
                    )
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
    async def test_get_mcp_tools_default_url(self, isolated_agent_tools):
        """Test that default URL is used when MCP_SERVER_URL is not set."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(return_value=[])
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
    async def test_get_mcp_tools_custom_url(self, isolated_agent_tools):
        """Test with custom MCP_SERVER_URL."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(return_value=[])
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
    async def test_get_mcp_tools_server_name(self, isolated_agent_tools):
        """Test that server name is 'data-layer'."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(return_value=[])
                    mock_client_class.return_value = mock_client

                    await get_mcp_tools()

                    # Verify server name is 'data-layer'
                    call_args = mock_client_class.call_args[0][0]
                    assert "data-layer" in call_args
                    assert call_args["data-layer"]["transport"] == "sse"

    @pytest.mark.asyncio
    async def test_get_mcp_tools_transport(self, isolated_agent_tools):
        """Test that transport is 'sse'."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(return_value=[])
                    mock_client_class.return_value = mock_client

                    await get_mcp_tools()

                    # Verify transport is 'sse'
                    call_args = mock_client_class.call_args[0][0]
                    assert call_args["data-layer"]["transport"] == "sse"

    @pytest.mark.asyncio
    async def test_get_mcp_tools_connection_error(self, isolated_agent_tools):
        """Test error handling for connection failures."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(side_effect=Exception("Connection failed"))
                    mock_client_class.return_value = mock_client

                    # Verify exception is raised
                    with pytest.raises(Exception, match="Connection failed"):
                        await get_mcp_tools()

    @pytest.mark.asyncio
    async def test_get_mcp_tools_empty_result(self, isolated_agent_tools):
        """Test handling of empty tool list."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_client.get_tools = AsyncMock(return_value=[])
                    mock_client_class.return_value = mock_client

                    result = await get_mcp_tools()

                    assert result == []

    @pytest.mark.asyncio
    async def test_get_mcp_tools_partial_tools(self, isolated_agent_tools):
        """Test handling when only some tools are available."""
        module = isolated_agent_tools
        get_mcp_tools = module.get_mcp_tools

        with patch.object(module, "MultiServerMCPClient") as mock_client_class:
            with patch.object(module, "load_dotenv"):
                with patch.dict(os.environ, {}, clear=True):
                    mock_client = AsyncMock()
                    mock_tool1 = MagicMock()
                    mock_tool1.name = "list_tables"
                    mock_tool2 = MagicMock()
                    mock_tool2.name = "execute_sql_query"
                    mock_client.get_tools = AsyncMock(return_value=[mock_tool1, mock_tool2])
                    mock_client_class.return_value = mock_client

                    result = await get_mcp_tools()

                    # assert 24 == 2 is fixed because mock is used
                    assert len(result) == 2
                    tool_names = [tool.name for tool in result]
                    assert "list_tables" in tool_names
                    assert "execute_sql_query" in tool_names

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from streamlit_app.service.admin import AdminService


@pytest.fixture(autouse=True)
def mock_mcp_deps():
    """Mock MCP dependencies to avoid ModuleNotFoundError."""
    mcp_mock = MagicMock()
    mcp_mock.__path__ = []
    mocks = {
        "mcp": mcp_mock,
        "mcp.types": MagicMock(),
        "mcp.client": MagicMock(),
        "mcp.client.sse": MagicMock(),
    }
    with patch.dict("sys.modules", mocks):
        yield


@pytest.mark.asyncio
async def test_call_tool_unwraps_result():
    """Test that _call_tool unwraps the {"result": ...} wrapper."""
    mock_raw_result = {"result": ["item1", "item2"]}

    # We need to mock the tools context and the tool itself
    mock_tool = AsyncMock()
    mock_tool.name = "some_tool"
    mock_tool.ainvoke.return_value = mock_raw_result

    with (
        patch("agent_core.tools.mcp_tools_context") as mock_ctx,
        patch("agent_core.tools.unpack_mcp_result", side_effect=lambda x: x),
    ):

        # Async context manager mock
        cm = MagicMock()
        cm.__aenter__.return_value = [mock_tool]
        mock_ctx.return_value = cm

        # In admin.py, it imports locally, so it should pick up our patches
        result = await AdminService._call_tool("some_tool", {})

        assert result == ["item1", "item2"]


@pytest.mark.asyncio
async def test_list_interactions_integration_with_fix():
    """Integration test within AdminService: _call_tool -> list_interactions."""
    mock_raw_result = {
        "result": [
            {"id": "1", "thumb": "UP", "execution_status": "APPROVED", "created_at": "2023-01-02"},
        ]
    }

    mock_tool = AsyncMock()
    mock_tool.name = "list_interactions"
    mock_tool.ainvoke.return_value = mock_raw_result

    with (
        patch("agent_core.tools.mcp_tools_context") as mock_ctx,
        patch("agent_core.tools.unpack_mcp_result", side_effect=lambda x: x),
    ):

        cm = MagicMock()
        cm.__aenter__.return_value = [mock_tool]
        mock_ctx.return_value = cm

        # This calls the REAL _call_tool which should now unwrap the result
        results = await AdminService.list_interactions(limit=10)

        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["id"] == "1"


@pytest.mark.asyncio
async def test_list_approved_examples_integration_with_fix():
    """Integration test within AdminService: _call_tool -> list_approved_examples."""
    mock_raw_result = {
        "result": [
            {"question": "How many films?", "sql_query": "SELECT count(*) FROM film"},
        ]
    }

    mock_tool = AsyncMock()
    mock_tool.name = "list_approved_examples"
    mock_tool.ainvoke.return_value = mock_raw_result

    with (
        patch("agent_core.tools.mcp_tools_context") as mock_ctx,
        patch("agent_core.tools.unpack_mcp_result", side_effect=lambda x: x),
    ):

        cm = MagicMock()
        cm.__aenter__.return_value = [mock_tool]
        mock_ctx.return_value = cm

        results = await AdminService.list_approved_examples(limit=10)

        assert isinstance(results, list)
        assert len(results) == 1
        assert results[0]["question"] == "How many films?"

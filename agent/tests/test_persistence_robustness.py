import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

# Set dummy env vars for the test to avoid import errors or config issues
os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5001"
# Patch mlflow to avoid actual connection attempts
with patch("mlflow.set_tracking_uri"), patch("mlflow.langchain.autolog"):
    from agent_core.graph import run_agent_with_tracing


@pytest.mark.asyncio
async def test_run_agent_persists_on_crash():
    """Test that update_interaction is called even if agent workflow crashes."""
    # Mock MCP tools
    mock_create_tool = AsyncMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke.return_value = "interaction-123"

    mock_update_tool = AsyncMock()
    mock_update_tool.name = "update_interaction"
    mock_update_tool.ainvoke.return_value = "OK"

    mock_tools = [mock_create_tool, mock_update_tool]

    # Mock app.ainvoke to raise an exception
    mock_app = AsyncMock()
    mock_app.ainvoke.side_effect = RuntimeError("Simulated Crash")

    # Patch dependencies
    with patch("agent_core.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)), patch(
        "agent_core.graph.app", mock_app
    ), patch("agent_core.graph.telemetry.update_current_trace"), patch(
        "agent_core.graph.telemetry"
    ):

        # Run the agent
        result = await run_agent_with_tracing("My question")

        # Verify result contains error
        assert result["error"] == "Simulated Crash"
        assert result["error_category"] == "SYSTEM_CRASH"

        # Verify create_interaction was called
        mock_create_tool.ainvoke.assert_awaited_once()

        # Verify update_interaction was called despite crash
        mock_update_tool.ainvoke.assert_awaited_once()

        # Verify update payload contains error
        call_args = mock_update_tool.ainvoke.call_args[0][0]
        assert call_args["interaction_id"] == "interaction-123"
        assert call_args["execution_status"] == "FAILURE"
        assert call_args["error_type"] == "SYSTEM_CRASH"
        assert "Simulated Crash" in call_args["response_payload"]


@pytest.mark.asyncio
async def test_run_agent_persists_on_success():
    """Test that update_interaction is called on success."""
    # Mock MCP tools
    mock_create_tool = AsyncMock()
    mock_create_tool.name = "create_interaction"
    mock_create_tool.ainvoke.return_value = "interaction-456"

    mock_update_tool = AsyncMock()
    mock_update_tool.name = "update_interaction"
    mock_update_tool.ainvoke.return_value = "OK"

    mock_tools = [mock_create_tool, mock_update_tool]

    # Mock app.ainvoke to return success
    mock_app = AsyncMock()
    mock_app.ainvoke.return_value = {
        "messages": [HumanMessage(content="Hello"), MagicMock(content="World")],
        "current_sql": "SELECT * FROM table",
        "error": None,
    }

    # Patch dependencies
    with patch("agent_core.tools.get_mcp_tools", new=AsyncMock(return_value=mock_tools)), patch(
        "agent_core.graph.app", mock_app
    ), patch("agent_core.graph.telemetry.update_current_trace"), patch(
        "agent_core.graph.telemetry"
    ):

        # Run the agent
        result = await run_agent_with_tracing("My question")

        # Verify success
        assert result["current_sql"] == "SELECT * FROM table"

        # Verify update_interaction was called
        mock_update_tool.ainvoke.assert_awaited_once()

        # Verify update payload
        call_args = mock_update_tool.ainvoke.call_args[0][0]
        assert call_args["interaction_id"] == "interaction-456"
        assert call_args["execution_status"] == "SUCCESS"
        assert call_args["generated_sql"] == "SELECT * FROM table"
        assert "World" in call_args["response_payload"]

"""Tests for structured ErrorMetadata propagation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.correct import correct_sql_node
from agent.nodes.execute import validate_and_execute_node
from agent.nodes.synthesize import synthesize_insight_node
from agent.state import AgentState


@pytest.mark.asyncio
async def test_execute_node_preserves_metadata():
    """Verify execute node preserves error_metadata from tool."""
    mock_tool_output = {
        "error": "Syntax error near something",
        "error_category": "syntax",
        "error_metadata": {
            "sql_state": "42601",
            "hint": "Check your quotes",
            "provider": "postgres",
        },
    }

    state = AgentState(
        messages=[HumanMessage(content="test")], current_sql="SELECT * FROM", retry_count=0
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools") as mock_get_tools,
        patch("agent.nodes.execute.PolicyEnforcer.validate_sql", return_value=None),
        patch("agent.nodes.execute.TenantRewriter.rewrite_sql", side_effect=lambda sql, tid: sql),
    ):
        mock_tool = MagicMock()
        mock_tool.name = "execute_sql_query"
        mock_tool.ainvoke = AsyncMock(return_value=[mock_tool_output])
        mock_get_tools.return_value = [mock_tool]

        result = await validate_and_execute_node(state)

        assert result["error_metadata"] == mock_tool_output["error_metadata"]
        assert result["error_category"] == "syntax"


@pytest.mark.asyncio
async def test_correct_node_uses_structured_category():
    """Verify correct node uses structured category from state."""
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT * FROM products",
        error='relation "products" does not exist',
        error_category="missing_join",  # Override default syntax classification
        error_metadata={"sql_state": "42P01", "provider": "postgres"},
        retry_count=0,
    )

    # get_llm is imported inside the function from agent.llm_client
    with (
        patch("agent.llm_client.get_llm") as mock_get_llm,
        patch("agent.nodes.correct.generate_correction_strategy") as mock_strategy,
    ):
        mock_chain = MagicMock()
        mock_get_llm.return_value = mock_chain
        mock_chain.invoke.return_value.content = "SELECT 1"
        mock_strategy.return_value = "strategy"

        correct_sql_node(state)

        # Check that generate_correction_strategy was called with metadata
        mock_strategy.assert_called_once()
        kwargs = mock_strategy.call_args[1]
        assert kwargs["error_metadata"] == state["error_metadata"]


@pytest.mark.asyncio
async def test_synthesize_node_handles_error():
    """Verify synthesize node can explain structured errors."""
    state = AgentState(
        messages=[HumanMessage(content="test")],
        query_result=None,
        error="Requested capability not supported",
        error_category="unsupported_capability",
        error_metadata={"required_capability": "pagination"},
        retry_count=3,
    )

    result = synthesize_insight_node(state)
    msg = result["messages"][0].content
    assert "unsupported_capability" not in msg  # Should be user friendly
    assert "pagination" in msg

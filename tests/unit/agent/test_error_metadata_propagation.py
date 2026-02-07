"""Tests for error metadata propagation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _envelope(rows=None, metadata=None, error=None, error_metadata=None):
    data = {
        "schema_version": "1.0",
        "rows": rows if rows is not None else [],
        "metadata": (
            metadata
            if metadata
            else {"rows_returned": len(rows) if rows else 0, "is_truncated": False}
        ),
    }
    if error:
        data["error"] = error
        if error_metadata:
            # Merge error metadata into error object if provided as dict
            if isinstance(error, dict):
                data["error"].update(error_metadata)
    return json.dumps(data)


@pytest.mark.asyncio
async def test_execute_node_preserves_metadata():
    """Verify execute node preserves error_metadata from tool."""
    mock_tool_error_meta = {
        "sql_state": "42601",
        "hint": "Check your quotes",
        "provider": "postgres",
    }
    mock_tool_error = {
        "message": "Syntax error near something",
        "category": "syntax",
        "is_retryable": False,
    }

    mock_payload = _envelope(error=mock_tool_error, error_metadata=mock_tool_error_meta)

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
        mock_tool.ainvoke = AsyncMock(return_value=mock_payload)
        mock_get_tools.return_value = [mock_tool]

        result = await validate_and_execute_node(state)

        # The agent extracts error metadata from the error object
        # It puts it into `error_metadata` output
        assert result["error_metadata"]["sql_state"] == "42601"
        assert result["error_metadata"]["hint"] == "Check your quotes"
        assert result["error_metadata"]["provider"] == "postgres"

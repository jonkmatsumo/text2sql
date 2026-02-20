"""Tests for canonical error-code passthrough into agent state."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _error_envelope(error: dict) -> str:
    return json.dumps(
        {
            "schema_version": "1.0",
            "rows": [],
            "metadata": {"rows_returned": 0, "is_truncated": False},
            "error": error,
        }
    )


@pytest.mark.asyncio
async def test_execute_node_preserves_canonical_error_code_from_tool_envelope():
    """Agent execute node should preserve canonical error_code from tool envelopes."""
    payload = _error_envelope(
        {
            "message": "Execution timed out.",
            "category": "timeout",
            "code": "DRIVER_TIMEOUT",
            "error_code": "DB_TIMEOUT",
            "retryable": True,
        }
    )
    state = AgentState(
        messages=[HumanMessage(content="test")],
        current_sql="SELECT 1",
        tenant_id=1,
        retry_count=0,
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools") as mock_get_tools,
        patch("agent.nodes.execute.PolicyEnforcer.validate_sql", return_value=None),
        patch("agent.nodes.execute.TenantRewriter.rewrite_sql", side_effect=lambda sql, tid: sql),
    ):
        tool = MagicMock()
        tool.name = "execute_sql_query"
        tool.ainvoke = AsyncMock(return_value=payload)
        mock_get_tools.return_value = [tool]

        result = await validate_and_execute_node(state)

    assert result["error_category"] == "timeout"
    assert result["error_metadata"]["error_code"] == "DB_TIMEOUT"
    assert result["error_metadata"]["code"] == "DRIVER_TIMEOUT"

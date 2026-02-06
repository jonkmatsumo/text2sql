"""Tests for agent pagination passthrough."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


def _mock_span_ctx(mock_start_span):
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_span


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_passthrough_page_token(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Agent should pass page_token and page_size to execute tool."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps(
        {
            "rows": [{"id": 1}],
            "metadata": {
                "is_truncated": False,
                "row_limit": 0,
                "rows_returned": 1,
                "next_page_token": "next-token",
                "page_size": 20,
            },
        }
    )
    mock_tool.ainvoke = AsyncMock(return_value=payload)
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_token="token-1",
        page_size=20,
    )

    result = await validate_and_execute_node(state)
    call_args = mock_tool.ainvoke.call_args[0][0]
    assert call_args["page_token"] == "token-1"
    assert call_args["page_size"] == 20
    completeness = result["result_completeness"]
    assert completeness["next_page_token"] == "next-token"

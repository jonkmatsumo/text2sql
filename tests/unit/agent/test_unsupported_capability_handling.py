"""Tests for unsupported capability handling in the agent."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.graph import route_after_execution
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
async def test_execute_unsupported_capability_message(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Unsupported capability errors should surface a stable message."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps(
        {
            "error": "Requested capability is not supported: pagination.",
            "error_category": "unsupported_capability",
            "required_capability": "pagination",
            "provider": "postgres",
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
    )

    result = await validate_and_execute_node(state)
    assert result["error_category"] == "unsupported_capability"
    assert "pagination" in result["error"]
    assert result["error_metadata"] == {
        "required_capability": "pagination",
        "provider": "postgres",
    }


def test_route_after_execution_skips_retry_for_unsupported():
    """Unsupported capability errors should fail fast without retries."""
    state = {
        "error": "unsupported",
        "error_category": "unsupported_capability",
        "retry_count": 0,
    }
    assert route_after_execution(state) == "failed"

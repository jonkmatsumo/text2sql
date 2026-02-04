"""P0 response envelope validation integration coverage."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.telemetry.get_current_trace_id")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_integration_malformed_payload_propagates_error(
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
    mock_trace_id,
    mock_start_span,
    schema_fixture,
):
    """Malformed tool payloads should fail closed with diagnostics."""
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
    mock_trace_id.return_value = "d" * 32

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=json.dumps({"unexpected": "shape"}))
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

    assert result["query_result"] is None
    assert result["error_category"] == "tool_response_malformed"
    assert "Trace ID" in result["error"]

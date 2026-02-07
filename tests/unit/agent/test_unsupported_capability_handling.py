"""Tests for unsupported capability handling in execute node."""

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
            if isinstance(error, dict):
                data["error"].update(error_metadata)
        if "rows_returned" not in data["metadata"]:
            data["metadata"]["rows_returned"] = 0
    return json.dumps(data)


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

    error_data = {
        "message": "Requested capability is not supported: pagination.",
        "category": "unsupported_capability",
        "provider": "postgres",
        "is_retryable": False,
    }
    error_meta = {
        "required_capability": "pagination",
        "capability_required": "pagination",
        "capability_supported": False,
        "fallback_policy": "off",
        "fallback_applied": False,
        "fallback_mode": "force_limited_results",
    }
    payload = _envelope(error=error_data, error_metadata=error_meta)

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

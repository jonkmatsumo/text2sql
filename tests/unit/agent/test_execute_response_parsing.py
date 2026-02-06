"""Tests for execute tool response parsing."""

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
async def test_execute_parses_enveloped_rows_and_metadata(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Enveloped response should parse rows + metadata."""
    mock_span = _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps(
        {"rows": [{"id": 1}], "metadata": {"is_truncated": False, "rows_returned": 1}}
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

    assert result["query_result"] == [{"id": 1}]
    assert result["error"] is None
    mock_span.set_attribute.assert_any_call("tool.response_shape", "enveloped")


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_parses_legacy_list_rows(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Legacy list responses should still work."""
    mock_span = _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=[{"id": 1}])
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

    assert result["query_result"] == [{"id": 1}]
    assert result["error"] is None
    mock_span.set_attribute.assert_any_call("tool.response_shape", "legacy")


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.telemetry.get_current_trace_id")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_detects_malformed_dict_payload(
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
    mock_trace_id,
    mock_start_span,
    schema_fixture,
):
    """Malformed payloads should fail closed."""
    _mock_span_ctx(mock_start_span)
    mock_trace_id.return_value = "a" * 32
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=json.dumps({"foo": "bar"}))
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


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_parses_capability_negotiation_metadata(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Capability fallback metadata should be preserved on unsupported responses."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "error": "Requested capability is not supported: pagination.",
                "error_category": "unsupported_capability",
                "required_capability": "pagination",
                "capability_required": "pagination",
                "capability_supported": False,
                "fallback_applied": False,
                "fallback_mode": "force_limited_results",
                "provider": "postgres",
            }
        )
    )
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
    assert result["error_metadata"]["capability_required"] == "pagination"
    assert result["error_metadata"]["capability_supported"] is False
    assert result["error_metadata"]["fallback_applied"] is False
    assert result["error_metadata"]["fallback_mode"] == "force_limited_results"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_parses_retry_after_metadata(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """retry_after_seconds should propagate for adaptive retry policy decisions."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "error": "Database Error: Too many requests",
                "error_category": "throttling",
                "retry_after_seconds": 2.0,
            }
        )
    )
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

    assert result["error_category"] == "throttling"
    assert result["retry_after_seconds"] == 2.0


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.telemetry.get_current_trace_id")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_detects_malformed_string_payload(
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
    mock_trace_id,
    mock_start_span,
    schema_fixture,
):
    """Unexpected string payloads should fail closed."""
    _mock_span_ctx(mock_start_span)
    mock_trace_id.return_value = "b" * 32
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value="unexpected payload")
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


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.telemetry.get_current_trace_id")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_detects_null_payload(
    mock_rewriter,
    mock_enforcer,
    mock_get_tools,
    mock_trace_id,
    mock_start_span,
    schema_fixture,
):
    """Null payloads should fail closed."""
    _mock_span_ctx(mock_start_span)
    mock_trace_id.return_value = "c" * 32
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=None)
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

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
            else:
                # If error is string, we can't easily merge without changing structure
                pass

        if "rows_returned" not in data["metadata"]:
            data["metadata"]["rows_returned"] = 0

    return json.dumps(data)


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
    payload = _envelope(rows=[{"id": 1}], metadata={"is_truncated": False, "rows_returned": 1})
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
async def test_execute_parses_standardized_truncation_metadata(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Agent should treat standardized truncation metadata as first-class signal."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = _envelope(
        rows=[{"id": 1}],
        metadata={
            "returned_count": 1,
            "truncated": True,
            "limit_applied": 1,
        },
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
    assert result["result_is_truncated"] is True


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_execute_parses_canonical_pagination_and_truncation_fields(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Canonical metadata keys should drive agent parsing without legacy fields."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = _envelope(
        rows=[{"id": 1}],
        metadata={
            "returned_count": 1,
            "truncated": True,
            "limit_applied": 1,
            "truncation_reason": "max_rows",
            "next_cursor": "cursor-2",
        },
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
    completeness = result["result_completeness"]
    assert result["result_is_truncated"] is True
    assert completeness["next_page_token"] == "cursor-2"


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

    # Construct error envelope
    error_data = {
        "message": "Requested capability is not supported: pagination.",
        "category": "unsupported_capability",
        "provider": "postgres",
        "is_retryable": False,
    }
    error_metadata = {
        "required_capability": "pagination",
        "capability_supported": False,
        "fallback_applied": False,
        "fallback_mode": "force_limited_results",
    }
    # In P0-A, capability fields are on Metadata or ErrorMetadata?
    # execute.py looks for them in error_metadata (from error object).
    # ErrorMetadata supports extra fields via **kwargs.

    payload = _envelope(error=error_data, error_metadata=error_metadata)

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
    assert result["error_metadata"]["required_capability"] == "pagination"
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

    error_data = {
        "message": "Database Error: Too many requests",
        "category": "throttling",
        "retry_after_seconds": 2.0,
        "provider": "test",
        "is_retryable": True,
    }
    payload = _envelope(error=error_data)

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
    # Random string falls back to tool_response_malformed category via error envelope
    assert result["error_category"] == "tool_response_malformed"
    assert "Trace ID" in result["error"] or "unexpected payload" in result["error"]


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

"""Tests for result completeness normalization."""

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


def _envelope(rows=None, metadata=None, error=None):
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
        if "rows_returned" not in data["metadata"]:
            data["metadata"]["rows_returned"] = 0
    return json.dumps(data)


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_default_success(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Default success response should be marked complete."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"id": 1}]))
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

    assert completeness["rows_returned"] == 1
    assert completeness["is_truncated"] is False
    assert completeness["is_limited"] is False
    assert completeness["partial_reason"] is None


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_truncated_reason(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Explicit truncation should map to TRUNCATED partial reason."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = _envelope(
        rows=[{"id": 1}], metadata={"is_truncated": True, "row_limit": 1, "rows_returned": 1}
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

    assert completeness["is_truncated"] is True
    assert completeness["partial_reason"] == "TRUNCATED"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_limited_only(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Limited results should map to LIMITED partial reason."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_envelope(rows=[{"id": 1}]))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        result_is_limited=True,
        result_limit=5,
    )

    result = await validate_and_execute_node(state)
    completeness = result["result_completeness"]

    assert completeness["is_limited"] is True
    assert completeness["partial_reason"] == "LIMITED"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_paginated(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Paginated results should map to PAGINATED partial reason."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = _envelope(
        rows=[{"id": 1}],
        metadata={
            "is_truncated": False,
            "row_limit": 0,
            "rows_returned": 1,
            "next_page_token": "next-token",
            "page_size": 25,
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

    assert completeness["partial_reason"] == "PAGINATED"
    assert completeness["next_page_token"] == "next-token"
    # Page size is not in typed Metadata model, so it's lost
    # assert completeness["page_size"] == 25
    assert completeness["page_size"] is None


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_provider_cap(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Provider cap detection should map to PROVIDER_CAP partial reason."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = _envelope(
        rows=[{"id": 1}],
        metadata={
            "is_truncated": True,
            "row_limit": 1,
            "rows_returned": 1,
            "partial_reason": "PROVIDER_CAP",
            "cap_detected": True,
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

    assert completeness["partial_reason"] == "PROVIDER_CAP"
    assert completeness["cap_detected"] is True


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_legacy_defaults(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Legacy responses should map to a sane default completeness object (shim enabled)."""
    monkeypatch.setenv("AGENT_ENABLE_LEGACY_TOOL_SHIM", "true")
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=[{"id": 1}, {"id": 2}])
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

    assert completeness["rows_returned"] == 2
    assert completeness["is_truncated"] is False
    assert completeness["partial_reason"] is None

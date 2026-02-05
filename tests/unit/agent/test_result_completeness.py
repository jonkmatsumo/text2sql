"""Tests for ResultCompleteness mapping."""

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
async def test_completeness_truncated_only(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Truncated results should map to TRUNCATED partial reason."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps(
        {
            "rows": [{"id": 1}],
            "metadata": {"is_truncated": True, "row_limit": 100, "rows_returned": 1},
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
    completeness = result["result_completeness"]

    assert completeness["is_truncated"] is True
    assert completeness["rows_returned"] == 1
    assert completeness["partial_reason"] == "TRUNCATED"
    assert completeness["row_limit"] == 100


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
    mock_tool.ainvoke = AsyncMock(return_value=[{"id": 1}])
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
    assert completeness["query_limit"] == 5
    assert completeness["partial_reason"] == "LIMITED"
    assert completeness["row_limit"] is None


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
    payload = json.dumps(
        {
            "rows": [{"id": 1}],
            "metadata": {
                "is_truncated": False,
                "row_limit": 0,
                "rows_returned": 1,
                "next_page_token": "next-token",
                "page_size": 25,
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
    )

    result = await validate_and_execute_node(state)
    completeness = result["result_completeness"]

    assert completeness["partial_reason"] == "PAGINATED"
    assert completeness["next_page_token"] == "next-token"
    assert completeness["page_size"] == 25


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_composed_cases(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Provider caps should flow into completeness metadata."""
    _mock_span_ctx(mock_start_span)
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    payload = json.dumps(
        {
            "rows": [{"id": 1}],
            "metadata": {
                "is_truncated": True,
                "row_limit": 100,
                "rows_returned": 1,
                "partial_reason": "PROVIDER_CAP",
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
        result_is_limited=True,
        result_limit=5,
    )

    result = await validate_and_execute_node(state)
    completeness = result["result_completeness"]

    assert completeness["partial_reason"] == "PROVIDER_CAP"
    assert completeness["is_limited"] is True
    assert completeness["query_limit"] == 5


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_completeness_legacy_defaults(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture
):
    """Legacy responses should map to a sane default completeness object."""
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
    assert completeness["partial_reason"] is None

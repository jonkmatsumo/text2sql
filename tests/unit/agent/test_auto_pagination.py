"""Tests for agent-driven automatic pagination loops."""

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


def _enveloped(rows, next_page_token, page_size=2):
    return json.dumps(
        {
            "rows": rows,
            "metadata": {
                "is_truncated": False,
                "row_limit": 0,
                "rows_returned": len(rows),
                "next_page_token": next_page_token,
                "page_size": page_size,
            },
        }
    )


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_stops_at_max_pages(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Auto-pagination should stop at configured max pages."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_PAGES", "2")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_ROWS", "50")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        side_effect=[
            _enveloped([{"id": 1}], "token-2"),
            _enveloped([{"id": 2}], "token-3"),
        ]
    )
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
    )

    result = await validate_and_execute_node(state)

    assert result["query_result"] == [{"id": 1}, {"id": 2}]
    assert mock_tool.ainvoke.call_count == 2
    second_call = mock_tool.ainvoke.call_args_list[1][0][0]
    assert second_call["page_token"] == "token-2"
    completeness = result["result_completeness"]
    assert completeness["auto_paginated"] is True
    assert completeness["pages_fetched"] == 2
    assert completeness["auto_pagination_stopped_reason"] == "max_pages"
    assert completeness["next_page_token"] == "token-3"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_stops_at_max_rows(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Auto-pagination should stop when max row budget is reached."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_PAGES", "5")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_ROWS", "2")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        side_effect=[
            _enveloped([{"id": 1}], "token-2"),
            _enveloped([{"id": 2}], "token-3"),
        ]
    )
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
    )

    result = await validate_and_execute_node(state)

    assert result["query_result"] == [{"id": 1}, {"id": 2}]
    assert mock_tool.ainvoke.call_count == 2
    completeness = result["result_completeness"]
    assert completeness["auto_paginated"] is True
    assert completeness["pages_fetched"] == 2
    assert completeness["auto_pagination_stopped_reason"] == "max_rows"
    assert completeness["next_page_token"] == "token-3"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_honors_next_page_tokens(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Auto-pagination should chain calls using returned next_page_token values."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_PAGES", "5")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_ROWS", "10")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        side_effect=[
            _enveloped([{"id": 1}], "token-2"),
            _enveloped([{"id": 2}], "token-3"),
            _enveloped([{"id": 3}], None),
        ]
    )
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
    )

    result = await validate_and_execute_node(state)

    assert result["query_result"] == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert mock_tool.ainvoke.call_count == 3
    second_call = mock_tool.ainvoke.call_args_list[1][0][0]
    third_call = mock_tool.ainvoke.call_args_list[2][0][0]
    assert second_call["page_token"] == "token-2"
    assert third_call["page_token"] == "token-3"
    completeness = result["result_completeness"]
    assert completeness["auto_paginated"] is True
    assert completeness["pages_fetched"] == 3
    assert completeness["auto_pagination_stopped_reason"] == "no_next_page"
    assert completeness["next_page_token"] is None


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_skips_when_provider_lacks_capability(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Unsupported pagination providers should not trigger an auto-pagination loop."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")
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
        page_size=2,
    )

    result = await validate_and_execute_node(state)

    assert result["error_category"] == "unsupported_capability"
    assert mock_tool.ainvoke.call_count == 1

"""Tests for auto-pagination hardening edge cases."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_token_repeat_guard(
    mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """Detect and break on token repetition."""
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION_MAX_PAGES", "5")

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"

    # Return same token repeatedly
    mock_tool.ainvoke.side_effect = [
        json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {
                    "next_page_token": "repeat-me",
                    "rows_returned": 1,
                    "response_shape": "enveloped",
                },
            }
        ),
        json.dumps(
            {
                "rows": [{"id": 2}],
                "metadata": {
                    "next_page_token": "repeat-me",
                    "rows_returned": 1,
                    "response_shape": "enveloped",
                },
            }
        ),
    ]
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["result_auto_pagination_stopped_reason"] == "token_repeat"
    assert len(result["query_result"]) == 2
    assert result["result_pages_fetched"] == 2


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_empty_page_with_token_guard(
    mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """Stop if we see multiple empty pages with tokens (pathological)."""
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"

    # Return empty rows but new token
    mock_tool.ainvoke.side_effect = [
        json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {
                    "next_page_token": "t1",
                    "rows_returned": 1,
                    "response_shape": "enveloped",
                },
            }
        ),
        json.dumps(
            {
                "rows": [],
                "metadata": {
                    "next_page_token": "t2",
                    "rows_returned": 0,
                    "response_shape": "enveloped",
                },
            }
        ),
        json.dumps(
            {
                "rows": [],
                "metadata": {
                    "next_page_token": "t3",
                    "rows_returned": 0,
                    "response_shape": "enveloped",
                },
            }
        ),
    ]
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["result_auto_pagination_stopped_reason"] == "pathological_empty_pages"
    assert len(result["query_result"]) == 1


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_auto_pagination_mid_loop_failure_discloses_partial(
    mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """Ensure partial aggregation is returned if a middle fetch fails."""
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"

    # First success, second error
    mock_tool.ainvoke.side_effect = [
        json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {
                    "next_page_token": "t1",
                    "rows_returned": 1,
                    "response_shape": "enveloped",
                },
            }
        ),
        json.dumps(
            {
                "error": "transient database error",
                "error_category": "transient",
                "response_shape": "error",
            }
        ),
    ]
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
    )

    result = await validate_and_execute_node(state)

    assert result["result_auto_pagination_stopped_reason"] == "fetch_error"
    assert len(result["query_result"]) == 1
    assert result["result_pages_fetched"] == 1

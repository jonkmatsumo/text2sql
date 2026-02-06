"""Tests for prefetch hardening and suppression logic."""

from unittest.mock import AsyncMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState
from agent.utils.pagination_prefetch import build_prefetch_cache_key, reset_prefetch_state


@pytest.fixture(autouse=True)
def cleanup_prefetch():
    """Reset prefetch state before and after each test."""
    reset_prefetch_state()
    yield
    reset_prefetch_state()


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_prefetch_suppressed_when_auto_pagination_active(
    mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """Prefetch should be suppressed if auto-pagination was active."""
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "on")

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    # Return two pages, loop will run, then prefetch check should see it's active/enabled
    mock_tool.ainvoke.side_effect = [
        # Page 1
        (
            '{"rows": [{"id": 1}], "metadata": {"next_page_token": "t1", '
            '"rows_returned": 1, "response_shape": "enveloped"}}'
        ),
        # Page 2
        (
            '{"rows": [{"id": 2}], "metadata": {"next_page_token": null, '
            '"rows_returned": 1, "response_shape": "enveloped"}}'
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
        interactive_session=True,
    )

    result = await validate_and_execute_node(state)
    assert result["result_prefetch_scheduled"] is False
    assert result["result_prefetch_reason"] == "auto_pagination_active"


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
@patch("agent.nodes.execute.time.monotonic")
async def test_prefetch_suppressed_low_budget(
    mock_monotonic, mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """Prefetch should be suppressed if time budget is low."""
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "on")

    # 0.0 at start, 0.6 before prefetch check
    mock_monotonic.side_effect = [0.0, 0.0, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6, 0.6]

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke.return_value = (
        '{"rows": [{"id": 1}], "metadata": {"next_page_token": "t1", '
        '"rows_returned": 1, "response_shape": "enveloped"}}'
    )
    mock_get_tools.return_value = [mock_tool]

    # Set deadline to 1.0 (so 1.0 - 0.6 = 0.4 which is < 0.5)
    deadline = 1.0

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql="SELECT * FROM users",
        query_result=None,
        error=None,
        retry_count=0,
        interactive_session=True,
        deadline_ts=deadline,
        page_size=10,
    )

    result = await validate_and_execute_node(state)
    assert result["result_prefetch_scheduled"] is False
    assert result["result_prefetch_reason"] == "low_budget"


def test_prefetch_cache_key_includes_flags(monkeypatch):
    """Cache key must change if configuration flags change."""
    sql = "SELECT 1"

    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "off")
    key1 = build_prefetch_cache_key(
        sql_query=sql,
        tenant_id=1,
        page_token="t",
        page_size=10,
        schema_snapshot_id="s",
        seed=1,
        completeness_hint=None,
    )

    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "apply")
    key2 = build_prefetch_cache_key(
        sql_query=sql,
        tenant_id=1,
        page_token="t",
        page_size=10,
        schema_snapshot_id="s",
        seed=1,
        completeness_hint=None,
    )

    assert key1 != key2

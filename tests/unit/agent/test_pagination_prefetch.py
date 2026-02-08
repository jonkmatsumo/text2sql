"""Tests for background pagination prefetch behavior."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.execute import validate_and_execute_node
from agent.state import AgentState
from agent.utils.pagination_prefetch import (
    PrefetchManager,
    prefetch_diagnostics,
    reset_prefetch_state,
)


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


@pytest.fixture(autouse=True)
def _reset_prefetch_fixture():
    reset_prefetch_state()
    yield
    reset_prefetch_state()


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_prefetch_disabled_respects_flag(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Prefetch should not run when AGENT_PREFETCH_NEXT_PAGE=off."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "off")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_enveloped([{"id": 1}], "token-2"))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
        interactive_session=True,
    )

    result = await validate_and_execute_node(state)
    # No need to await wait_for_prefetch_tasks, node handles it.

    assert mock_tool.ainvoke.call_count == 1
    completeness = result["result_completeness"]
    assert completeness["prefetch_enabled"] is False
    assert completeness["prefetch_scheduled"] is False
    assert completeness["prefetch_reason"] == "disabled"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_prefetch_skips_non_interactive_sessions(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Prefetch should stay off for non-interactive execution contexts."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(return_value=_enveloped([{"id": 1}], "token-2"))
    mock_get_tools.return_value = [mock_tool]

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
        interactive_session=False,
    )

    result = await validate_and_execute_node(state)

    assert mock_tool.ainvoke.call_count == 1
    completeness = result["result_completeness"]
    assert completeness["prefetch_enabled"] is False
    assert completeness["prefetch_scheduled"] is False
    assert completeness["prefetch_reason"] == "non_interactive"


@pytest.mark.asyncio
@patch("agent.nodes.execute.telemetry.start_span")
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_prefetch_schedules_and_serves_cached_next_page(
    mock_rewriter, mock_enforcer, mock_get_tools, mock_start_span, schema_fixture, monkeypatch
):
    """Prefetch should cache the next page and serve it on the follow-up request."""
    _mock_span_ctx(mock_start_span)
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "on")
    monkeypatch.setenv("AGENT_PREFETCH_MAX_CONCURRENCY", "1")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")
    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke = AsyncMock(
        side_effect=[
            _enveloped([{"id": 1}], "token-2"),
            _enveloped([{"id": 2}], None),
        ]
    )
    mock_get_tools.return_value = [mock_tool]

    first_state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_size=2,
        interactive_session=True,
        schema_snapshot_id="snap-1",
    )

    first_result = await validate_and_execute_node(first_state)

    assert mock_tool.ainvoke.call_count == 2  # 1 for fetch, 1 for prefetch
    first_completeness = first_result["result_completeness"]
    assert first_completeness["prefetch_enabled"] is True
    assert first_completeness["prefetch_scheduled"] is True
    assert first_completeness["prefetch_reason"] == "scheduled"

    second_state = AgentState(
        messages=[],
        schema_context="",
        current_sql=schema_fixture.sample_query,
        query_result=None,
        error=None,
        retry_count=0,
        page_token="token-2",
        page_size=2,
        interactive_session=True,
        schema_snapshot_id="snap-1",
    )

    # Second call should use cache
    second_result = await validate_and_execute_node(second_state)

    # mock_tool should NOT be called again because of cache hit
    assert mock_tool.ainvoke.call_count == 2
    assert second_result["query_result"] == [{"id": 2}]
    assert second_result["result_completeness"]["prefetch_reason"] == "cache_hit"


@pytest.mark.asyncio
async def test_prefetch_concurrency_never_exceeds_configured_limit():
    """Background prefetch should obey configured concurrency limits."""
    concurrency_limit = 1

    async def _slow_fetch():
        await asyncio.sleep(0.05)
        return {"rows": [{"id": 1}], "metadata": {"next_page_token": None}}

    from common.constants.reason_codes import PrefetchSuppressionReason

    async with PrefetchManager(max_concurrency=concurrency_limit) as pm:
        scheduled1, reason1 = pm.schedule("key-1", _slow_fetch)
        assert scheduled1 is True
        assert reason1 == PrefetchSuppressionReason.SCHEDULED.value

        scheduled2, reason2 = pm.schedule("key-2", _slow_fetch)
        assert scheduled2 is True
        assert reason2 == PrefetchSuppressionReason.SCHEDULED.value

        scheduled3, reason3 = pm.schedule("key-3", _slow_fetch)
        assert scheduled3 is True
        assert reason3 == PrefetchSuppressionReason.SCHEDULED.value

        # Wait for tasks to complete
        pass

    diag = prefetch_diagnostics()
    assert diag["max_observed_concurrency"] <= concurrency_limit
    assert diag["active_count"] == 0


@pytest.mark.asyncio
async def test_prefetch_respects_global_limit(monkeypatch):
    """Prefetch should obey the project-wide global limit across multiple managers."""
    global_limit = 2
    monkeypatch.setenv("AGENT_PREFETCH_GLOBAL_LIMIT", str(global_limit))

    async def _slow_fetch():
        await asyncio.sleep(0.05)
        return {"rows": [{"id": 1}]}

    # Multiple managers should still be bound by the global limit
    pm1 = PrefetchManager(max_concurrency=5)
    pm2 = PrefetchManager(max_concurrency=5)

    async with pm1, pm2:
        pm1.schedule("key-1", _slow_fetch)
        pm1.schedule("key-2", _slow_fetch)
        pm2.schedule("key-3", _slow_fetch)
        pm2.schedule("key-4", _slow_fetch)

    diag = prefetch_diagnostics()
    # Even if managers allow 5 each, the global limit of 2 should hold
    assert diag["max_observed_concurrency"] <= global_limit
    assert diag["active_count"] == 0


@pytest.mark.asyncio
async def test_prefetch_storm_control_skips_when_waiters_exceeded(monkeypatch):
    """Prefetch should skip when the number of waiters exceeds the storm threshold."""
    monkeypatch.setenv("AGENT_PREFETCH_GLOBAL_LIMIT", "1")
    monkeypatch.setenv("AGENT_PREFETCH_STORM_THRESHOLD", "1")

    async def _very_slow_fetch():
        await asyncio.sleep(0.2)
        return {"rows": [{"id": 1}]}

    pm = PrefetchManager(max_concurrency=5)
    async with pm:
        # First task acquires local and global. Waiters=0, Active=1.
        pm.schedule("key-1", _very_slow_fetch)
        await asyncio.sleep(0.01)

        # Second task acquires local, waits for global. Waiters=1, Active=1.
        pm.schedule("key-2", _very_slow_fetch)
        await asyncio.sleep(0.01)

        diag = prefetch_diagnostics()
        assert diag["waiting_count"] == 1

        from common.constants.reason_codes import PrefetchSuppressionReason

        # Third task should be rejected by schedule() because waiters >= storm_threshold (1)
        scheduled, reason = pm.schedule("key-3", _very_slow_fetch)
        assert scheduled is False
        assert reason == PrefetchSuppressionReason.STORM_WAITERS.value


@pytest.mark.asyncio
async def test_prefetch_cooldown_skips_after_failure(monkeypatch):
    """Prefetch should skip if currently in failure cooldown."""
    monkeypatch.setenv("AGENT_PREFETCH_COOLDOWN_SECONDS", "30")

    async def _fail_fetch():
        raise RuntimeError("Immediate failure")

    async def _success_fetch():
        return {"rows": [{"id": 1}]}

    pm = PrefetchManager()
    async with pm:
        # Schedule a failing task
        pm.schedule("fail-key", _fail_fetch)

    diag = prefetch_diagnostics()
    assert diag["in_cooldown"] is True

    from common.constants.reason_codes import PrefetchSuppressionReason

    # Next schedule attempt should be rejected immediately due to cooldown
    async with pm:
        scheduled, reason = pm.schedule("success-key", _success_fetch)
        assert scheduled is False
        assert reason == PrefetchSuppressionReason.COOLDOWN_ACTIVE.value

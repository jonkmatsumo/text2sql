"""Tests for incident kill-switch behavior."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from agent.audit import AuditEventType, get_audit_event_buffer, reset_audit_event_buffer
from agent.graph import route_after_execution
from agent.nodes.execute import validate_and_execute_node
from agent.state.decision_summary import build_run_decision_summary


@pytest.fixture(autouse=True)
def _reset_audit():
    reset_audit_event_buffer()
    yield
    reset_audit_event_buffer()


def test_disable_llm_retries_kill_switch_overrides_retry_flow(monkeypatch):
    """DISABLE_LLM_RETRIES should force fast-fail and emit a kill-switch decision event."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")

    state = {
        "run_id": "run-disable-llm-retries",
        "error": "transient",
        "error_category": "connectivity",
        "retry_count": 0,
        "llm_retries_kill_switch_enabled": True,
    }

    route = route_after_execution(state)

    assert route == "failed"
    reasons = [event["reason"] for event in state.get("decision_events", [])]
    assert "kill_switch_disable_llm_retries" in reasons
    recent = get_audit_event_buffer().list_recent(limit=1)
    assert recent[0]["event_type"] == AuditEventType.KILL_SWITCH_OVERRIDE.value
    summary = build_run_decision_summary(state)
    assert summary["kill_switches"]["disable_llm_retries"] is True


def test_disable_schema_refresh_kill_switch_prevents_refresh(monkeypatch):
    """DISABLE_SCHEMA_REFRESH should block refresh routing and emit a decision event."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")

    state = {
        "run_id": "run-disable-schema-refresh",
        "error": "Schema drift detected",
        "error_category": "schema_drift",
        "retry_count": 0,
        "schema_drift_suspected": True,
        "schema_drift_auto_refresh": True,
        "schema_refresh_kill_switch_enabled": True,
    }

    route = route_after_execution(state)

    assert route != "refresh_schema"
    reasons = [event["reason"] for event in state.get("decision_events", [])]
    assert "kill_switch_disable_schema_refresh" in reasons
    recent = get_audit_event_buffer().list_recent(limit=1)
    assert recent[0]["event_type"] == AuditEventType.KILL_SWITCH_OVERRIDE.value
    summary = build_run_decision_summary(state)
    assert summary["kill_switches"]["disable_schema_refresh"] is True


@pytest.mark.asyncio
@patch("agent.nodes.execute.get_mcp_tools")
@patch("agent.nodes.execute.PolicyEnforcer")
@patch("agent.nodes.execute.TenantRewriter")
async def test_disable_prefetch_kill_switch_prevents_prefetch(
    mock_rewriter, mock_enforcer, mock_get_tools, monkeypatch
):
    """DISABLE_PREFETCH should suppress scheduling and record kill-switch event context."""
    monkeypatch.setenv("AGENT_PREFETCH_NEXT_PAGE", "on")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")

    mock_enforcer.validate_sql.return_value = None
    mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

    mock_tool = AsyncMock()
    mock_tool.name = "execute_sql_query"
    mock_tool.ainvoke.return_value = (
        '{"rows": [{"id": 1}], "metadata": {"next_page_token": "t1", '
        '"rows_returned": 1, "response_shape": "enveloped"}}'
    )
    mock_get_tools.return_value = [mock_tool]

    state = {
        "run_id": "run-disable-prefetch",
        "messages": [],
        "schema_context": "",
        "current_sql": "SELECT * FROM users",
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "interactive_session": True,
        "page_size": 10,
        "prefetch_kill_switch_enabled": True,
    }

    result = await validate_and_execute_node(state)

    assert result["result_prefetch_enabled"] is False
    assert result["result_prefetch_scheduled"] is False
    assert result["result_prefetch_reason"] == "disabled_kill_switch"
    reasons = [event["reason"] for event in state.get("decision_events", [])]
    assert "kill_switch_disable_prefetch" in reasons
    recent = get_audit_event_buffer().list_recent(limit=1)
    assert recent[0]["event_type"] == AuditEventType.KILL_SWITCH_OVERRIDE.value
    summary = build_run_decision_summary(state)
    assert summary["kill_switches"]["disable_prefetch"] is True

"""Tests for run-level decision summary artifacts."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.graph as graph_mod
from agent.audit import AuditEventType, get_audit_event_buffer, reset_audit_event_buffer
from agent.state.decision_summary import build_run_decision_summary


@asynccontextmanager
async def _mock_tools_context():
    yield []


def test_build_run_decision_summary_has_expected_shape_and_no_sensitive_fields():
    """Run summary should be bounded to operational fields only."""
    state = {
        "tenant_id": 7,
        "schema_snapshot_id": "snap-123",
        "retry_count": 2,
        "schema_refresh_count": 1,
        "prefetch_discard_count": 3,
        "error_category": "timeout",
        "error_metadata": {"category": "timeout"},
        "correction_attempts": [
            {"attempt": 1, "error_category": "syntax"},
            {"attempt": 2, "error_category": "timeout"},
        ],
        "termination_reason": "timeout",
        "current_sql": "SELECT * FROM secret_table",
        "query_result": [{"ssn": "123-45-6789"}],
    }

    summary = build_run_decision_summary(state, llm_calls=4, llm_token_total=321)
    summary_hash = summary.get("decision_summary_hash")

    assert isinstance(summary_hash, str)
    assert len(summary_hash) == 64
    assert {k: v for k, v in summary.items() if k != "decision_summary_hash"} == {
        "tenant_id": 7,
        "replay_mode": False,
        "schema_snapshot_id": "snap-123",
        "retries": 2,
        "llm_calls": 4,
        "llm_token_total": 321,
        "tool_calls": {"total": 0},
        "rows": {"total": 0},
        "budget_exceeded": {
            "llm": False,
            "tool_calls": False,
            "rows": False,
        },
        "schema_refresh_count": 1,
        "prefetch_discard_count": 3,
        "kill_switches": {
            "disable_prefetch": False,
            "disable_schema_refresh": False,
            "disable_llm_retries": False,
        },
        "decision_event_counts": {},
        "decision_events_truncated": False,
        "decision_events_dropped": 0,
        "error_categories_encountered": ["syntax", "timeout"],
        "terminated_reason": "timeout",
    }
    assert "current_sql" not in summary
    assert "query_result" not in summary
    assert summary["rows"]["total"] == 0


def test_build_run_decision_summary_records_replay_mode():
    """Run summary should capture replay_mode flag for deterministic forensics."""
    summary = build_run_decision_summary({"tenant_id": 3, "replay_mode": True})

    assert summary["tenant_id"] == 3
    assert summary["replay_mode"] is True


def test_run_decision_summary_hash_excludes_volatile_counters():
    """Decision summary hash should ignore volatile token and row counters."""
    base_state = {
        "tenant_id": 1,
        "retry_count": 0,
        "schema_snapshot_id": "snap-1",
    }
    summary_one = build_run_decision_summary(
        {**base_state, "tool_calls_total": 1, "rows_total": 10},
        llm_calls=2,
        llm_token_total=100,
    )
    summary_two = build_run_decision_summary(
        {**base_state, "tool_calls_total": 9, "rows_total": 99},
        llm_calls=20,
        llm_token_total=500,
    )
    assert summary_one["decision_summary_hash"] == summary_two["decision_summary_hash"]


@pytest.mark.asyncio
async def test_run_agent_attaches_run_summary_and_emits_final_span_event(monkeypatch):
    """Agent entrypoint should attach run summary and emit final span summary event."""
    mock_current_span = MagicMock()
    monkeypatch.setattr(graph_mod.telemetry, "get_current_span", lambda: mock_current_span)

    run_state = {
        "messages": [],
        "tenant_id": 42,
        "retry_count": 1,
        "schema_refresh_count": 2,
        "prefetch_discard_count": 1,
        "error_category": "timeout",
        "termination_reason": "timeout",
        "current_sql": "SELECT * FROM secret_table",
        "query_result": [{"internal": "sensitive"}],
    }

    with patch.object(graph_mod, "app") as mock_app:
        mock_app.ainvoke = AsyncMock(return_value=run_state)

        with patch("agent.tools.mcp_tools_context", side_effect=_mock_tools_context):
            result = await graph_mod.run_agent_with_tracing(
                "show me revenue",
                tenant_id=42,
                thread_id="thread-run-summary-test",
            )

    summary = result.get("run_decision_summary")
    assert summary is not None
    assert summary["tenant_id"] == 42
    assert summary["retries"] == 1
    assert summary["schema_refresh_count"] == 2
    assert summary["prefetch_discard_count"] == 1
    assert summary["terminated_reason"] == "timeout"
    assert summary["replay_mode"] is False
    assert summary["llm_calls"] >= 0
    assert summary["llm_token_total"] >= 0
    assert "timeout" in summary["error_categories_encountered"]
    assert "current_sql" not in summary
    assert "query_result" not in summary

    emitted_events = [
        call.args[0] for call in mock_current_span.add_event.call_args_list if call.args
    ]
    assert "agent.run_decision_summary" in emitted_events


@pytest.mark.asyncio
async def test_run_agent_replay_mode_emits_audit_event(monkeypatch):
    """Replay-mode executions should emit a structured replay activation audit event."""
    reset_audit_event_buffer()

    with patch.object(graph_mod, "app") as mock_app:
        mock_app.ainvoke = AsyncMock(
            return_value={
                "messages": [],
                "tenant_id": 9,
                "retry_count": 0,
                "schema_refresh_count": 0,
                "prefetch_discard_count": 0,
                "termination_reason": "success",
                "error_category": None,
            }
        )
        with patch("agent.tools.mcp_tools_context", side_effect=_mock_tools_context):
            await graph_mod.run_agent_with_tracing(
                "replay this",
                tenant_id=9,
                thread_id="thread-replay-audit",
                replay_mode=True,
                replay_bundle={"tool_io": []},
            )

    recent = get_audit_event_buffer().list_recent(limit=1)
    assert recent[0]["event_type"] == AuditEventType.REPLAY_MODE_ACTIVATED.value
    assert recent[0]["tenant_id"] == 9
    reset_audit_event_buffer()

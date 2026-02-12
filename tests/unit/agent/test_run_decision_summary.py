"""Tests for run-level decision summary artifacts."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.graph as graph_mod
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

    assert summary == {
        "tenant_id": 7,
        "schema_snapshot_id": "snap-123",
        "retries": 2,
        "llm_calls": 4,
        "llm_token_total": 321,
        "schema_refresh_count": 1,
        "prefetch_discard_count": 3,
        "error_categories_encountered": ["syntax", "timeout"],
        "terminated_reason": "timeout",
    }
    assert "current_sql" not in summary
    assert "query_result" not in summary
    assert "rows" not in summary


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
    assert summary["llm_calls"] >= 0
    assert summary["llm_token_total"] >= 0
    assert "timeout" in summary["error_categories_encountered"]
    assert "current_sql" not in summary
    assert "query_result" not in summary

    emitted_events = [
        call.args[0] for call in mock_current_span.add_event.call_args_list if call.args
    ]
    assert "agent.run_decision_summary" in emitted_events

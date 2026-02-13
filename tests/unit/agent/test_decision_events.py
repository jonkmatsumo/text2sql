"""Tests for structured decision-event emission and retention."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agent.graph import route_after_execution
from agent.nodes.correct import correct_sql_node
from agent.state.decision_events import append_decision_event


def test_route_emits_retry_and_fail_decision_events(monkeypatch):
    """Retry and fail routing paths should emit structured decision events."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    retry_state = {
        "run_id": "run-retry",
        "error": "transient",
        "error_category": "timeout",
        "retry_count": 0,
        "deadline_ts": 110.0,
        "ema_llm_latency_seconds": 1.0,
    }
    retry_route = route_after_execution(retry_state)
    assert retry_route == "correct"
    retry_event = retry_state["decision_events"][-1]
    assert retry_event["decision"] == "retry"
    assert retry_event["reason"] in {"PROCEED_TO_CORRECTION", "THROTTLE_RETRY"}

    fail_state = {
        "run_id": "run-fail",
        "error": "permission denied",
        "error_category": "auth",
        "error_metadata": {"is_retryable": False},
        "retry_count": 0,
    }
    fail_route = route_after_execution(fail_state)
    assert fail_route == "failed"
    fail_event = fail_state["decision_events"][-1]
    assert fail_event["decision"] == "fail"
    assert fail_event["reason"] == "NON_RETRYABLE_CATEGORY"


def test_replay_mode_fast_fail_emits_decision_event(monkeypatch):
    """Replay mode should fast-fail retries and emit a fail decision event."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")

    state = {
        "run_id": "run-replay",
        "error": "retryable transient",
        "error_category": "connectivity",
        "retry_count": 0,
        "replay_mode": True,
    }

    route = route_after_execution(state)
    assert route == "failed"
    event = state["decision_events"][-1]
    assert event["decision"] == "fail"
    assert event["reason"] == "MAX_RETRIES_REACHED"


@patch("agent.llm_client.get_llm")
@patch("agent.nodes.correct.ChatPromptTemplate")
def test_correct_node_emits_throttle_sleep_event(mock_prompt_class, mock_llm, monkeypatch):
    """Correction path should emit throttle_sleep events when retry_after is applied."""
    monkeypatch.setattr("agent.nodes.correct.time.sleep", lambda _: None)

    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    mock_prompt_class.from_messages.return_value = mock_prompt

    mock_response = MagicMock()
    mock_response.content = "SELECT 1"
    mock_chain.invoke.return_value = mock_response

    state = {
        "run_id": "run-sleep",
        "messages": [],
        "schema_context": "",
        "current_sql": "SELECT x",
        "error": "timeout",
        "error_category": "timeout",
        "retry_count": 0,
        "retry_after_seconds": 1.2,
    }

    _ = correct_sql_node(state)
    sleep_event = state["decision_events"][-1]
    assert sleep_event["decision"] == "throttle_sleep"
    assert sleep_event["reason"] == "retry_after_sleep"
    assert sleep_event["retry_after_seconds"] == pytest.approx(1.2, rel=0, abs=1e-6)


def test_decision_event_retention_is_fifo_bounded(monkeypatch):
    """Decision events should evict oldest entries when over configured bounds."""
    monkeypatch.setenv("AGENT_DECISION_EVENTS_MAX_EVENTS", "2")
    state = {"run_id": "run-fifo"}

    append_decision_event(
        state,
        node="node_a",
        decision="retry",
        reason="first",
        retry_count=0,
    )
    append_decision_event(
        state,
        node="node_b",
        decision="retry",
        reason="second",
        retry_count=1,
    )
    append_decision_event(
        state,
        node="node_c",
        decision="fail",
        reason="third",
        retry_count=2,
    )

    reasons = [event["reason"] for event in state["decision_events"]]
    assert reasons == ["second", "third"]
    assert state["decision_events_truncated"] is True
    assert state["decision_events_dropped"] >= 1

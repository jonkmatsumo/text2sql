"""Tests for adaptive retry policy behavior."""

from agent.graph import route_after_execution


def test_adaptive_policy_stops_non_retryable_categories(monkeypatch):
    """Adaptive retry should fail fast on non-retryable error categories."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")

    state = {
        "error": "permission denied",
        "error_category": "auth",
        "retry_count": 0,
    }

    result = route_after_execution(state)

    assert result == "failed"
    assert state["retry_summary"]["policy"] == "adaptive"
    assert state["retry_summary"]["stopped_non_retryable"] is True


def test_adaptive_policy_keeps_retry_after_within_budget(monkeypatch):
    """Retry-after should be respected and bounded by remaining budget."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "0")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    state = {
        "error": "temporarily unavailable",
        "error_category": "connectivity",
        "retry_count": 0,
        "deadline_ts": 110.0,
        "latency_correct_seconds": 2.0,
        "retry_after_seconds": 1.5,
    }

    result = route_after_execution(state)

    assert result == "correct"
    assert state["retry_after_seconds"] == 1.5
    assert state["retry_summary"]["retry_after_seconds"] == 1.5


def test_adaptive_policy_fails_when_retry_after_exhausts_budget(monkeypatch):
    """Retry-after should not exceed remaining time budget."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "0")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    state = {
        "error": "temporarily unavailable",
        "error_category": "connectivity",
        "retry_count": 0,
        "deadline_ts": 102.0,
        "latency_correct_seconds": 2.0,
        "retry_after_seconds": 5.0,
    }

    result = route_after_execution(state)

    assert result == "failed"
    assert state["error_category"] == "timeout"
    assert state["retry_summary"]["budget_exhausted"] is True


def test_max_retries_can_be_configured(monkeypatch):
    """Retry ceiling should follow AGENT_MAX_RETRIES when configured."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "static")
    monkeypatch.setenv("AGENT_MAX_RETRIES", "1")

    state = {
        "error": "transient",
        "error_category": "connectivity",
        "retry_count": 1,
    }

    result = route_after_execution(state)

    assert result == "failed"
    assert state["retry_summary"]["max_retries_reached"] is True


def test_retry_decision_event_emitted(monkeypatch):
    """Ensure structured retry decision event is added to span."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    from unittest.mock import MagicMock

    mock_span = MagicMock()
    monkeypatch.setattr("agent.graph.telemetry.get_current_span", lambda: mock_span)

    state = {
        "error": "timeout",
        "error_category": "timeout",
        "retry_count": 0,
        "deadline_ts": 110.0,
        "ema_llm_latency_seconds": 2.0,
    }

    route_after_execution(state)

    mock_span.add_event.assert_called()
    # Check that retry.decision was among events
    found = False
    for call in mock_span.add_event.call_args_list:
        if call[0][0] == "retry.decision":
            found = True
            payload = call[0][1]
            assert payload["reason_code"] == "PROCEED_TO_CORRECTION"
            assert payload["will_retry"] is True
            assert payload["policy"] == "adaptive"
    assert found


def test_retry_suppressed_insufficient_budget(monkeypatch):
    """Adaptive retry should stop if estimated budget exceeds remaining time."""
    monkeypatch.setenv("AGENT_RETRY_POLICY", "adaptive")
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "5")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    from unittest.mock import MagicMock

    mock_span = MagicMock()
    monkeypatch.setattr("agent.graph.telemetry.get_current_span", lambda: mock_span)

    state = {
        "error": "transient",
        "error_category": "connectivity",
        "retry_count": 0,
        "deadline_ts": 104.0,  # only 4s left, but min budget is 5s
    }

    result = route_after_execution(state)
    assert result == "failed"

    # Verify reason code
    found = False
    for call in mock_span.add_event.call_args_list:
        if call[0][0] == "retry.decision":
            found = True
            assert call[0][1]["reason_code"] == "INSUFFICIENT_BUDGET"
    assert found

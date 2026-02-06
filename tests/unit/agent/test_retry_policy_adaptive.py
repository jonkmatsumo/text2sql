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

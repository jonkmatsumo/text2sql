"""Tests for retry budget enforcement."""

from agent.graph import route_after_execution


def test_retry_loop_stops_when_budget_insufficient(monkeypatch):
    """Ensure retries stop when remaining budget is too small."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "5")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    state = {
        "error": "Execution error",
        "retry_count": 0,
        "deadline_ts": 102.0,
    }

    result = route_after_execution(state)

    assert result == "failed"
    assert "Retry budget exhausted" in state["error"]


def test_retry_loop_continues_when_budget_sufficient(monkeypatch):
    """Ensure retries continue when remaining budget is sufficient."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "5")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    state = {
        "error": "Execution error",
        "retry_count": 0,
        "deadline_ts": 110.0,
    }

    result = route_after_execution(state)

    assert result == "correct"

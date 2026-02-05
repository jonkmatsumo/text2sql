"""P0 retry budget estimation tests."""

from unittest.mock import MagicMock, patch

import pytest

from agent.graph import _estimate_correction_budget_seconds, route_after_execution
from agent.nodes.correct import correct_sql_node
from agent.state import AgentState


def test_estimate_uses_observed_latency_with_overhead(monkeypatch):
    """Estimate should include observed latency plus overhead."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "0")
    state = {"latency_correct_seconds": 1.2}

    estimate = _estimate_correction_budget_seconds(state)

    assert estimate >= 1.7


def test_estimate_respects_min_budget(monkeypatch):
    """Estimate should not fall below configured min budget."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "5")
    state = {"latency_correct_seconds": 1.0}

    estimate = _estimate_correction_budget_seconds(state)

    assert estimate >= 5.0


@pytest.mark.asyncio
async def test_integration_slow_correction_influences_budget(monkeypatch):
    """Slow correction latency should prevent retries when budget is tight."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "0")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 10.0)

    state = AgentState(
        messages=[],
        schema_context="",
        current_sql="SELECT 1",
        query_result=None,
        error="Execution error",
        retry_count=0,
    )

    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    mock_response = MagicMock()
    mock_response.content = "SELECT 1"
    mock_chain.invoke.return_value = mock_response

    with (
        patch("agent.nodes.correct.ChatPromptTemplate") as mock_prompt_class,
        patch("agent.nodes.correct.telemetry.start_span") as mock_span,
        patch("agent.nodes.correct.time.monotonic", side_effect=[0.0, 5.0]),
    ):
        mock_prompt_class.from_messages.return_value = mock_prompt
        mock_span.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_span.return_value.__exit__ = MagicMock(return_value=False)
        result = correct_sql_node(state)

    result["deadline_ts"] = 14.0
    result["error"] = "Execution error"

    decision = route_after_execution(result)

    assert decision == "failed"
    assert "estimated" in result["error"]

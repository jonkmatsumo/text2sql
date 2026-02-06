"""Tests for retry budget estimation using EMA latency."""

import pytest

from agent.graph import _estimate_correction_budget_seconds
from agent.state import AgentState


@pytest.fixture
def base_state():
    """Return a valid base AgentState for testing."""
    return AgentState(
        ema_llm_latency_seconds=None,
        latency_generate_seconds=None,
        latency_correct_seconds=None,
        retry_count=0,
    )


def test_estimate_budget_fallback_default(base_state, mocker):
    """Test fallback when no latency history is available."""
    # Mock env vars to defaults
    mocker.patch("agent.graph.get_env_float", return_value=3.0)

    # No EMA, no observed latency
    budget = _estimate_correction_budget_seconds(base_state)

    # default (3.0) + overhead (0.5) = 3.5, but min is 3.0.
    # Wait, implementation says: if ema is None => ema = min_budget or 3.0.
    # estimated = ema + 0.5 = 3.5.
    # max(3.5, 3.0) = 3.5.
    assert budget == 3.5


def test_estimate_budget_warm_start_generate(base_state, mocker):
    """Test warm start from generate latency."""
    mocker.patch("agent.graph.get_env_float", return_value=3.0)

    base_state["latency_generate_seconds"] = 10.0

    budget = _estimate_correction_budget_seconds(base_state)

    # EMA initialized from observed (10.0).
    # estimated = 10.0 + 0.5 = 10.5
    assert budget == 10.5


def test_estimate_budget_warm_start_correct(base_state, mocker):
    """Test warm start from correct latency (preferred if available?)."""
    mocker.patch("agent.graph.get_env_float", return_value=3.0)

    base_state["latency_generate_seconds"] = 5.0
    base_state["latency_correct_seconds"] = 8.0  # later observation

    # Implementation uses state.get("latency_correct_seconds") or ...
    # So it prefers correct.
    budget = _estimate_correction_budget_seconds(base_state)

    # estimated = 8.0 + 0.5 = 8.5
    assert budget == 8.5


def test_estimate_budget_use_ema(base_state, mocker):
    """Test using existing EMA value."""
    mocker.patch("agent.graph.get_env_float", return_value=3.0)

    base_state["ema_llm_latency_seconds"] = 12.0
    base_state["latency_generate_seconds"] = 20.0  # Ignored if EMA present

    budget = _estimate_correction_budget_seconds(base_state)

    # estimated = 12.0 + 0.5 = 12.5
    assert budget == 12.5


def test_estimate_budget_enforce_min(base_state, mocker):
    """Test that minimum budget floor is enforced."""
    mocker.patch("agent.graph.get_env_float", return_value=5.0)  # High min

    base_state["ema_llm_latency_seconds"] = 1.0  # Fast previous query

    budget = _estimate_correction_budget_seconds(base_state)

    # estimated = 1.0 + 0.5 = 1.5
    # max(1.5, 5.0) = 5.0
    # Wait, implementation: if min_budget: estimated = max(estimated, float(min_budget))
    assert budget == 5.0

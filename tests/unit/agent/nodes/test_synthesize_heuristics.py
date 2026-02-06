"""Tests for synthesize_insight heuristics."""

from unittest.mock import MagicMock, patch

import pytest

from agent.nodes.synthesize import synthesize_insight_node
from agent.state import AgentState


@pytest.fixture
def mock_telemetry(mocker):
    """Mock telemetry to prevent emission."""
    return mocker.patch("agent.nodes.synthesize.telemetry")


@pytest.fixture
def mock_llm_chain(mocker):
    """Mock LLM chain."""
    # Mock get_llm to prevent real LLM calls
    mock_llm = MagicMock()
    mocker.patch("agent.nodes.synthesize.get_llm", return_value=mock_llm)
    return mock_llm


def test_synthesize_empty_with_filters_suggests_check(mock_telemetry, mock_llm_chain):
    """Test that empty results with WHERE clause triggers suggestion."""
    state = AgentState(
        messages=[],
        query_result=[],  # Empty
        current_sql="SELECT * FROM users WHERE age > 100",  # Has filters
        schema_drift_suspected=False,
    )

    # We need to mock _sanity_check_enabled to True?
    # Or rely on default? I plan to change default to True.
    # But for test robustness I should patch it.

    with patch("agent.nodes.synthesize._sanity_check_enabled", return_value=True):
        result = synthesize_insight_node(state)
        content = result["messages"][0].content

        assert "couldn't find any rows" in content
        assert "double-check" in content or "verify" in content


def test_synthesize_empty_no_filters_no_suggestion(mock_telemetry, mock_llm_chain):
    """Test that empty results without filters does NOT trigger specific suggestion."""
    state = AgentState(
        messages=[],
        query_result=[],
        current_sql="SELECT * FROM users",  # No filters
        schema_drift_suspected=False,
    )

    with patch("agent.nodes.synthesize._sanity_check_enabled", return_value=True):
        result = synthesize_insight_node(state)
        content = result["messages"][0].content

        assert "couldn't find any rows" in content
        # Should NOT suggest checking filters since there are none
        assert "double-check filters" not in content


def test_synthesize_empty_implies_existence(mock_telemetry, mock_llm_chain):
    """Test that 'top/best' questions trigger suggestion."""
    # We need a HumanMessage in messages
    from langchain_core.messages import HumanMessage

    state = AgentState(
        messages=[HumanMessage(content="Show me the top users")],
        query_result=[],
        current_sql="SELECT * FROM users",
        schema_drift_suspected=False,
    )

    with patch("agent.nodes.synthesize._sanity_check_enabled", return_value=True):
        result = synthesize_insight_node(state)
        content = result["messages"][0].content

        # _question_implies_existence should capture 'top'
        assert "If you expected results" in content or "double-check filters" in content

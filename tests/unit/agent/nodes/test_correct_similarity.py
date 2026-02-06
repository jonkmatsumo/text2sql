"""Tests for correct_sql_node similarity enforcement."""

from unittest.mock import MagicMock, patch

import pytest  # noqa: F401

from agent.nodes.correct import correct_sql_node
from agent.state import AgentState


@patch("agent.nodes.correct.get_env_bool")
@patch("agent.utils.sql_similarity.compute_sql_similarity")
@patch("agent.llm_client.get_llm")
def test_correct_enforces_similarity_success(mock_get_llm, mock_sim, mock_env_bool, mocker):
    """Test that correction passes if similarity is high enough."""
    mock_env_bool.return_value = True
    mock_sim.return_value = 0.9  # High similarity

    # Mock LLM response
    mock_llm = MagicMock()
    mock_response = MagicMock()
    mock_response.content = "SELECT * FROM corrected"
    # Invoke returns the response
    mock_llm.invoke.return_value = mock_response

    # The code does: chain = prompt | get_llm
    # We mock get_llm to return mock_llm.
    mock_get_llm.return_value = mock_llm

    # We also need to mock prompt | llm.
    # We can mock ChatPromptTemplate so that __or__ returns a mock chain.
    mock_chain = MagicMock()
    mock_chain.invoke.return_value = mock_response

    with patch("agent.nodes.correct.ChatPromptTemplate") as mock_prompt_cls:
        mock_prompt = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_prompt
        # When prompt | llm is called
        mock_prompt.__or__.return_value = mock_chain

        state = AgentState(
            messages=[], current_sql="SELECT * FROM original", error="Syntax error", retry_count=0
        )

        result = correct_sql_node(state)

        assert result["current_sql"] == "SELECT * FROM corrected"
        assert result["error"] is None
        assert mock_sim.call_count == 1


@patch("agent.nodes.correct.get_env_bool")
@patch("agent.utils.sql_similarity.compute_sql_similarity")
@patch("agent.llm_client.get_llm")
def test_correct_rejects_drift(mock_get_llm, mock_sim, mock_env_bool, mocker):
    """Test that correction is rejected if similarity is low, then original returned on fallback."""
    mock_env_bool.return_value = True
    # First attempt low similarity (0.1)
    mock_sim.return_value = 0.1

    mock_response = MagicMock()
    mock_response.content = "SELECT * FROM driftwood"

    # Mock chain
    mock_chain = MagicMock()
    mock_chain.invoke.return_value = mock_response

    with patch("agent.nodes.correct.ChatPromptTemplate") as mock_prompt_cls:
        mock_prompt = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_prompt
        mock_prompt.__or__.return_value = mock_chain

        mock_get_llm.return_value = MagicMock()

        state = AgentState(
            messages=[], current_sql="SELECT * FROM original", error="Syntax error", retry_count=0
        )

        result = correct_sql_node(state)

        # Expect retry loop runs max_drift_retries (1) + initial (1) = 2, then gives up.
        # But wait, code says:
        # if drift_attempts < max_drift_retries (1): drift_attempts++ continue
        # else: return failed
        # 1. Initial attempt -> Drift -> attempts=1 -> continue
        # 2. Retry attempt -> Drift -> attempts=1 (check < 1 is false) -> return failed
        # So total 2 calls.

        assert mock_chain.invoke.call_count == 2

        # Result should be original SQL + correction_drift error
        assert result["current_sql"] == "SELECT * FROM original"
        assert result["error_category"] == "correction_drift"
        assert "drift detected" in result["error"]


@patch("agent.nodes.correct.get_env_bool")
@patch("agent.utils.sql_similarity.compute_sql_similarity")
@patch("agent.llm_client.get_llm")
def test_correct_retry_succeeds(mock_get_llm, mock_sim, mock_env_bool, mocker):
    """Test that correction succeeds on retry."""
    mock_env_bool.return_value = True
    # First call 0.1 (fail), Second call 0.9 (success)
    mock_sim.side_effect = [0.1, 0.9]

    mock_response = MagicMock()
    mock_response.content = "SELECT * FROM final"

    mock_chain = MagicMock()
    mock_chain.invoke.return_value = mock_response

    with patch("agent.nodes.correct.ChatPromptTemplate") as mock_prompt_cls:
        mock_prompt = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_prompt
        mock_prompt.__or__.return_value = mock_chain

        mock_get_llm.return_value = MagicMock()

        state = AgentState(
            messages=[], current_sql="SELECT * FROM original", error="Syntax error", retry_count=0
        )

        result = correct_sql_node(state)

        # 1. Initial attempt -> Drift -> retry
        # 2. Retry attempt -> Success -> break
        assert mock_chain.invoke.call_count == 2

        assert result["current_sql"] == "SELECT * FROM final"
        assert result["error"] is None

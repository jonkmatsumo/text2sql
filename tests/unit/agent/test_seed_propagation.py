"""Tests for deterministic seed propagation."""

from unittest.mock import MagicMock, patch

import pytest

from agent.llm_client import get_llm
from agent.nodes.generate import generate_sql_node


def test_get_llm_cache_keys_with_seed():
    """Test that get_llm caches clients separately for different seeds."""
    with patch("agent.llm_client.get_llm_client") as mock_get_client:
        mock_get_client.return_value = MagicMock()

        # Call with different seeds
        get_llm(seed=123)
        get_llm(seed=456)
        get_llm(seed=123)  # Should be cached

        assert mock_get_client.call_count == 2


@pytest.mark.asyncio
async def test_generate_node_propagates_seed():
    """Test that generate_sql_node passes the seed from state to get_llm."""
    state = {
        "messages": [MagicMock(content="test question")],
        "schema_context": "dummy schema",
        "seed": 999,
        "ema_llm_latency_seconds": 1.0,
    }

    mock_llm = MagicMock()
    mock_llm.invoke.return_value = MagicMock(content="SELECT 1")

    with patch("agent.llm_client.get_llm", return_value=mock_llm) as mock_get_llm:
        await generate_sql_node(state)

        # Verify get_llm was called with the seed from state
        mock_get_llm.assert_called_with(temperature=0, seed=999)


def test_llm_telemetry_captures_seed():
    """Test that the LLM wrapper records the seed in telemetry."""
    from agent.llm_client import _wrap_llm

    mock_llm = MagicMock()
    mock_llm.invoke.return_value = MagicMock(content="response")

    wrapped = _wrap_llm(mock_llm, seed=777)

    with patch("agent.telemetry.telemetry.start_span") as mock_start_span:
        mock_span = mock_start_span.return_value.__enter__.return_value

        wrapped.invoke("test input")

        # Verify seed was recorded as an attribute
        mock_span.set_attribute.assert_any_call("generation.seed", 777)

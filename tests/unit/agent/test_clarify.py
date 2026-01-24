"""Unit tests for clarify node with LangGraph interrupt."""

from unittest.mock import patch

import pytest

from agent.nodes.clarify import clarify_node


@pytest.fixture
def base_state():
    """Create a base agent state for testing."""
    return {
        "messages": [],
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": 1,
        "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
        "clarification_question": "Do you mean Customer region or Store region?",
    }


class TestClarifyNode:
    """Tests for clarify_node function."""

    @pytest.mark.asyncio
    async def test_no_question_returns_empty(self, base_state):
        """Test that no clarification question returns empty dict."""
        base_state["clarification_question"] = None

        with patch("mlflow.start_span") as mock_span:
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await clarify_node(base_state)

        assert result == {}

    @pytest.mark.asyncio
    async def test_with_interrupt_available(self, base_state):
        """Test clarify node with interrupt available."""
        # Mock the interrupt function
        mock_user_response = "Customer region"

        with (
            patch("mlflow.start_span") as mock_span,
            patch("agent.nodes.clarify.interrupt", return_value=mock_user_response),
        ):
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await clarify_node(base_state)

        assert result.get("user_clarification") == "Customer region"
        assert result.get("ambiguity_type") is None
        assert result.get("clarification_question") is None

    @pytest.mark.asyncio
    async def test_without_interrupt_fallback(self, base_state):
        """Test clarify node falls back when interrupt not available."""
        with (
            patch("mlflow.start_span") as mock_span,
            patch("agent.nodes.clarify.interrupt", None),
        ):
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            result = await clarify_node(base_state)

        # Should proceed without clarification
        assert result.get("user_clarification") is None
        assert result.get("ambiguity_type") is None

    @pytest.mark.asyncio
    async def test_interrupt_receives_correct_payload(self, base_state):
        """Test that interrupt receives correct payload."""
        captured_payload = None

        def mock_interrupt(payload):
            nonlocal captured_payload
            captured_payload = payload
            return "User response"

        with (
            patch("mlflow.start_span") as mock_span,
            patch("agent.nodes.clarify.interrupt", mock_interrupt),
        ):
            mock_span.return_value.__enter__ = lambda s: type(
                "Span",
                (),
                {
                    "set_inputs": lambda *a, **k: None,
                    "set_outputs": lambda *a, **k: None,
                    "set_attribute": lambda *a, **k: None,
                },
            )()
            mock_span.return_value.__exit__ = lambda *a, **k: None

            await clarify_node(base_state)

        assert captured_payload is not None
        assert captured_payload["type"] == "clarification_needed"
        assert captured_payload["question"] == base_state["clarification_question"]
        assert captured_payload["ambiguity_type"] == base_state["ambiguity_type"]

"""Tests for retry observability attributes."""

from unittest.mock import MagicMock, patch

from agent.nodes.correct import correct_sql_node
from agent.state import AgentState
from agent.taxonomy.error_taxonomy import ERROR_TAXONOMY


def _mock_span_ctx(mock_start_span):
    mock_span = MagicMock()
    mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
    mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
    return mock_span


@patch("agent.llm_client.get_llm")
@patch("agent.nodes.correct.ChatPromptTemplate")
def test_retry_observability_emits_bounded_attributes(mock_prompt_class, mock_llm, monkeypatch):
    """Retry observability uses bounded reason categories."""
    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    mock_prompt_class.from_messages.return_value = mock_prompt

    mock_response = MagicMock()
    mock_response.content = "SELECT 1"
    mock_chain.invoke.return_value = mock_response

    monkeypatch.setenv("QUERY_TARGET_BACKEND", "postgres")

    with (
        patch("agent.nodes.correct.telemetry.start_span") as mock_start_span,
        patch("agent.nodes.correct.telemetry.get_current_span") as mock_get_span,
    ):
        mock_span = _mock_span_ctx(mock_start_span)
        mock_get_span.return_value = mock_span

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM bad",
            query_result=None,
            error="syntax error at or near FROM",
            retry_count=1,
        )

        correct_sql_node(state)

        expected_categories = set(ERROR_TAXONOMY.keys()) | {"UNKNOWN"}
        call_args = [
            c for c in mock_span.set_attribute.call_args_list if c[0][0] == "retry.reason_category"
        ]
        assert call_args, "retry.reason_category not set"
        reason_category = call_args[0][0][1]
        assert reason_category in expected_categories

        mock_span.add_event.assert_called_once()
        event_args = mock_span.add_event.call_args[0]
        assert event_args[0] == "agent.retry"
        attrs = event_args[1]
        assert attrs["stage"] == "correct_sql"
        assert attrs["reason_category"] in expected_categories

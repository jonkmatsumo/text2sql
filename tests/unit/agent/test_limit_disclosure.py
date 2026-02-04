"""Tests for limit disclosure and parsing."""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.synthesize import synthesize_insight_node
from agent.nodes.validate import validate_sql_node
from agent.state import AgentState


def _mock_span(mock_span):
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


@pytest.mark.asyncio
async def test_validate_sets_result_limit_from_limit_clause():
    """Ensure LIMIT clause sets result limit metadata."""
    base_state = {
        "messages": [HumanMessage(content="Show me the latest customers")],
        "schema_context": "Table: customers (id, name, created_at)",
        "table_names": ["customers"],
        "current_sql": "SELECT * FROM customers ORDER BY created_at DESC LIMIT 10",
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": 1,
    }

    with patch("agent.nodes.validate.telemetry.start_span") as mock_span:
        _mock_span(mock_span)
        result = await validate_sql_node(base_state)

    assert result.get("result_is_limited") is True
    assert result.get("result_limit") == 10


@patch("agent.llm_client.get_llm")
@patch("agent.nodes.synthesize.ChatPromptTemplate")
def test_synthesize_includes_limit_disclosure(mock_prompt_class, mock_llm):
    """Ensure limit disclosure is prepended to responses."""
    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    mock_prompt_class.from_messages.return_value = mock_prompt

    mock_response = MagicMock()
    mock_response.content = "Here are the top customers."
    mock_chain.invoke.return_value = mock_response

    state = AgentState(
        messages=[HumanMessage(content="Top customers")],
        schema_context="",
        current_sql="SELECT * FROM customers ORDER BY total_spend DESC LIMIT 5",
        query_result=[{"id": 1}],
        result_is_limited=True,
        result_limit=5,
        error=None,
        retry_count=0,
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content
    assert "limited to the top 5 rows" in content
    assert "Here are the top customers." in content


@patch("agent.llm_client.get_llm")
@patch("agent.nodes.synthesize.ChatPromptTemplate")
def test_synthesize_handles_limit_and_truncation_together(mock_prompt_class, mock_llm):
    """Ensure both limit and truncation disclosures appear."""
    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    mock_prompt_class.from_messages.return_value = mock_prompt

    mock_response = MagicMock()
    mock_response.content = "Results summary."
    mock_chain.invoke.return_value = mock_response

    state = AgentState(
        messages=[HumanMessage(content="Top customers")],
        schema_context="",
        current_sql="SELECT * FROM customers ORDER BY total_spend DESC LIMIT 5",
        query_result=[{"id": 1}],
        result_is_limited=True,
        result_limit=5,
        result_is_truncated=True,
        result_row_limit=100,
        result_rows_returned=100,
        error=None,
        retry_count=0,
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content
    assert "Results are truncated" in content
    assert "limited to the top 5 rows" in content

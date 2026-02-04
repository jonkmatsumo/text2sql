"""Tests for empty result messaging."""

import pytest

from agent.nodes.synthesize import synthesize_insight_node
from agent.state import AgentState


@pytest.mark.parametrize("schema_drift", [False, True])
def test_empty_results_message_includes_guidance(schema_drift):
    """Empty results should provide guidance and optional drift hint."""
    from langchain_core.messages import HumanMessage

    state = AgentState(
        messages=[HumanMessage(content="Show me orders")],
        schema_context="",
        current_sql="SELECT * FROM orders WHERE 1=0",
        query_result=[],
        error=None,
        retry_count=0,
        schema_drift_suspected=schema_drift,
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content
    assert "couldn't find any rows" in content.lower()
    assert "widening your filters" in content.lower()
    if schema_drift:
        assert "schema may have changed" in content.lower()


def test_empty_results_sanity_check_flag(monkeypatch):
    """Sanity check adds extra caution when enabled."""
    from langchain_core.messages import HumanMessage

    monkeypatch.setenv("AGENT_EMPTY_RESULT_SANITY_CHECK", "true")
    state = AgentState(
        messages=[HumanMessage(content="Top customers last month")],
        schema_context="",
        current_sql="SELECT * FROM customers WHERE 1=0",
        query_result=[],
        error=None,
        retry_count=0,
    )

    result = synthesize_insight_node(state)
    content = result["messages"][0].content.lower()
    assert "double-check filters" in content

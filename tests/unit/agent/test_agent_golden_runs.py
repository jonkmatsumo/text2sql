"""Golden agent-run tests for disclosure messaging."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage
from langgraph.graph import END, StateGraph

from agent.graph import route_after_cache_lookup, route_after_execution, route_after_validation
from agent.nodes.cache_lookup import cache_lookup_node
from agent.nodes.execute import validate_and_execute_node
from agent.nodes.synthesize import synthesize_insight_node
from agent.nodes.validate import validate_sql_node
from agent.state import AgentState


class _DummySpan:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def set_attribute(self, *_args, **_kwargs):
        return None

    def set_inputs(self, *_args, **_kwargs):
        return None

    def set_outputs(self, *_args, **_kwargs):
        return None

    def set_attributes(self, *_args, **_kwargs):
        return None


def _build_test_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("validate", validate_sql_node)
    workflow.add_node("execute", validate_and_execute_node)
    workflow.add_node("synthesize", synthesize_insight_node)
    workflow.set_entry_point("validate")
    workflow.add_edge("validate", "execute")
    workflow.add_edge("execute", "synthesize")
    workflow.add_edge("synthesize", END)
    return workflow.compile()


def _mock_llm(prompt_class):
    mock_prompt = MagicMock()
    mock_chain = MagicMock()
    mock_prompt.from_messages.return_value = mock_prompt
    mock_prompt.__or__ = MagicMock(return_value=mock_chain)
    prompt_class.from_messages.return_value = mock_prompt

    mock_response = MagicMock()
    mock_response.content = "Mock response."
    mock_chain.invoke.return_value = mock_response


@pytest.mark.asyncio
async def test_golden_truncation_disclosure(monkeypatch):
    """Truncation metadata should be disclosed in final response."""
    monkeypatch.setenv("AGENT_SYNTHESIZE_MODE", "deterministic")

    tool = AsyncMock()
    tool.name = "execute_sql_query"
    tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {"is_truncated": True, "row_limit": 100, "rows_returned": 100},
            }
        )
    )

    state = AgentState(
        messages=[HumanMessage(content="Show rows")],
        schema_context="",
        current_sql="SELECT * FROM items",
        query_result=None,
        error=None,
        retry_count=0,
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.nodes.validate.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.synthesize.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.synthesize.ChatPromptTemplate") as mock_prompt_class,
        patch("agent.llm_client.get_llm"),
    ):
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        _mock_llm(mock_prompt_class)
        result = await _build_test_graph().ainvoke(state)

    content = result["messages"][-1].content
    assert "Results are truncated" in content


@pytest.mark.asyncio
async def test_golden_limit_disclosure(monkeypatch):
    """Limit clause should be disclosed in final response."""
    monkeypatch.setenv("AGENT_SYNTHESIZE_MODE", "deterministic")

    tool = AsyncMock()
    tool.name = "execute_sql_query"
    tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "rows": [{"id": 1}],
                "metadata": {"is_truncated": False, "row_limit": 0, "rows_returned": 1},
            }
        )
    )

    state = AgentState(
        messages=[HumanMessage(content="Top items")],
        schema_context="",
        current_sql="SELECT * FROM items ORDER BY score DESC LIMIT 5",
        query_result=None,
        error=None,
        retry_count=0,
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.nodes.validate.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.synthesize.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.synthesize.ChatPromptTemplate") as mock_prompt_class,
        patch("agent.llm_client.get_llm"),
    ):
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        _mock_llm(mock_prompt_class)
        result = await _build_test_graph().ainvoke(state)

    content = result["messages"][-1].content
    assert "limited to the top 5 rows" in content


@pytest.mark.asyncio
async def test_golden_drift_hint_on_empty_results(monkeypatch):
    """Schema drift hint should appear with empty results when flagged."""
    monkeypatch.setenv("AGENT_SYNTHESIZE_MODE", "deterministic")

    tool = AsyncMock()
    tool.name = "execute_sql_query"
    tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {"rows": [], "metadata": {"is_truncated": False, "row_limit": 0, "rows_returned": 0}}
        )
    )

    state = AgentState(
        messages=[HumanMessage(content="Show rows")],
        schema_context="",
        current_sql="SELECT * FROM items",
        query_result=None,
        error=None,
        retry_count=0,
        schema_drift_suspected=True,
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.nodes.validate.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.synthesize.telemetry.start_span", return_value=_DummySpan()),
    ):
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        result = await _build_test_graph().ainvoke(state)

    content = result["messages"][-1].content
    assert "schema may have changed" in content.lower()


@pytest.mark.asyncio
async def test_golden_retry_budget_message(monkeypatch):
    """Retry budget exhaustion should set a clear error message."""
    monkeypatch.setenv("AGENT_MIN_RETRY_BUDGET_SECONDS", "0")
    monkeypatch.setattr("agent.graph.time.monotonic", lambda: 100.0)

    state = {
        "error": "Execution error",
        "retry_count": 0,
        "deadline_ts": 102.0,
        "latency_correct_seconds": 2.0,
    }

    result = route_after_execution(state)

    assert result == "failed"
    assert "Retry budget exhausted" in state["error"]
    assert "remaining time" in state["error"]
    assert "estimated" in state["error"]


@pytest.mark.asyncio
async def test_golden_malformed_tool_response_includes_trace_id(monkeypatch):
    """Malformed tool responses should surface trace id for diagnostics."""
    tool = AsyncMock()
    tool.name = "execute_sql_query"
    tool.ainvoke = AsyncMock(return_value=json.dumps({"unexpected": "shape"}))

    state = AgentState(
        messages=[HumanMessage(content="Show rows")],
        schema_context="",
        current_sql="SELECT * FROM items",
        query_result=None,
        error=None,
        retry_count=0,
    )

    with (
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[tool])),
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.execute.telemetry.get_current_trace_id", return_value="e" * 32),
    ):
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)
        result = await validate_and_execute_node(state)

    assert "Trace ID" in result["error"]


@pytest.mark.asyncio
async def test_golden_schema_binding_failure_skips_execute(monkeypatch):
    """Schema binding validation should prevent execution when enabled."""
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "true")

    raw_schema = [
        {"type": "Table", "name": "customers"},
        {"type": "Column", "name": "id", "table": "customers"},
    ]

    state = AgentState(
        messages=[HumanMessage(content="Show customers")],
        schema_context="",
        raw_schema_context=raw_schema,
        current_sql="SELECT customers.name FROM customers",
        query_result=None,
        error=None,
        retry_count=0,
    )

    workflow = StateGraph(AgentState)

    async def _correct_node(s):
        return s

    async def _execute_node(_state):
        raise AssertionError("execute node should not run on schema binding failure")

    workflow.add_node("validate", validate_sql_node)
    workflow.add_node("execute", _execute_node)
    workflow.add_node("correct", _correct_node)
    workflow.set_entry_point("validate")
    workflow.add_conditional_edges(
        "validate",
        route_after_validation,
        {"execute": "execute", "correct": "correct"},
    )
    workflow.add_edge("correct", END)
    workflow.add_edge("execute", END)

    with patch("agent.nodes.validate.telemetry.start_span", return_value=_DummySpan()):
        result = await workflow.compile().ainvoke(state)

    assert result.get("error_category") == "schema_binding"


@pytest.mark.asyncio
async def test_golden_cache_schema_mismatch_routes_to_retrieve(monkeypatch):
    """Cache hit rejected on schema mismatch should route to retrieval."""
    monkeypatch.setenv("AGENT_CACHE_SCHEMA_VALIDATION", "true")

    cache_tool = AsyncMock()
    cache_tool.name = "lookup_cache"
    cache_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            {
                "cache_id": "cache-1",
                "value": "SELECT 1",
                "similarity": 1.0,
                "metadata": {"schema_snapshot_id": "fp-old"},
            }
        )
    )

    subgraph_tool = AsyncMock()
    subgraph_tool.name = "get_semantic_subgraph"
    subgraph_tool.ainvoke = AsyncMock(
        return_value=json.dumps({"nodes": [{"type": "Table", "name": "t2"}]})
    )

    state = AgentState(
        messages=[HumanMessage(content="Show data")],
        schema_context="",
        current_sql=None,
        query_result=None,
        error=None,
        retry_count=0,
        tenant_id=1,
    )

    with (
        patch("agent.nodes.cache_lookup.telemetry.start_span", return_value=_DummySpan()),
        patch(
            "agent.nodes.cache_lookup.get_mcp_tools",
            AsyncMock(return_value=[cache_tool, subgraph_tool]),
        ),
        patch("agent.utils.schema_fingerprint.resolve_schema_snapshot_id", return_value="fp-new"),
    ):
        result = await cache_lookup_node(state)

    assert result["from_cache"] is False
    assert route_after_cache_lookup(result) == "retrieve"

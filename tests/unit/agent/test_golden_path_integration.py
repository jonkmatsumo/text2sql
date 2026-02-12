"""Golden-path integration test for schema retrieval through execution."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.execute import validate_and_execute_node
from agent.nodes.generate import generate_sql_node
from agent.nodes.plan import plan_sql_node
from agent.nodes.retrieve import retrieve_context_node
from agent.nodes.validate import validate_sql_node
from common.models.tool_envelopes import (
    ExecuteSQLQueryMetadata,
    ExecuteSQLQueryResponseEnvelope,
    GenericToolMetadata,
    ToolResponseEnvelope,
)


class _DummySpan:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def set_attribute(self, *_args, **_kwargs):
        return None

    def set_attributes(self, *_args, **_kwargs):
        return None

    def set_inputs(self, *_args, **_kwargs):
        return None

    def set_outputs(self, *_args, **_kwargs):
        return None

    def add_event(self, *_args, **_kwargs):
        return None


def _mock_prompt_response(mock_prompt_class, content: str) -> None:
    prompt = MagicMock()
    chain = MagicMock()
    prompt.from_messages.return_value = prompt
    prompt.__or__ = MagicMock(return_value=chain)
    mock_prompt_class.from_messages.return_value = prompt

    response = MagicMock()
    response.content = content
    response.response_metadata = {}
    chain.invoke.return_value = response


@pytest.mark.asyncio
async def test_golden_path_integration_covers_retrieve_generate_validate_execute(monkeypatch):
    """Golden path should cover retrieval, generation, validation, and execution."""
    monkeypatch.setenv("MCP_USER_ROLE", "SQL_ADMIN_ROLE,TABLE_ADMIN_ROLE,SQL_USER_ROLE")
    monkeypatch.setenv("AGENT_AUTO_PAGINATION", "off")
    monkeypatch.setenv("AGENT_SCHEMA_BINDING_VALIDATION", "false")

    subgraph_tool = AsyncMock()
    subgraph_tool.name = "get_semantic_subgraph"
    subgraph_payload = ToolResponseEnvelope(
        result={
            "nodes": [
                {"type": "Table", "name": "orders", "description": "Orders"},
                {"type": "Column", "table": "orders", "name": "id"},
                {"type": "Column", "table": "orders", "name": "total"},
            ],
            "relationships": [],
        },
        metadata=GenericToolMetadata(provider="semantic_layer"),
    )
    subgraph_tool.ainvoke = AsyncMock(
        return_value=subgraph_payload.model_dump_json(exclude_none=True)
    )

    recommend_tool = AsyncMock()
    recommend_tool.name = "recommend_examples"
    recommend_payload = ToolResponseEnvelope(
        result={
            "examples": [{"question": "show orders", "sql": "SELECT id, total FROM orders"}],
            "fallback_used": False,
            "metadata": {"count_total": 1},
        },
        metadata=GenericToolMetadata(provider="recommendation"),
    )
    recommend_tool.ainvoke = AsyncMock(
        return_value=recommend_payload.model_dump_json(exclude_none=True)
    )

    execute_tool = AsyncMock()
    execute_tool.name = "execute_sql_query"
    execute_payload = ExecuteSQLQueryResponseEnvelope(
        rows=[{"id": 1, "total": 100}],
        columns=[{"name": "id", "type": "integer"}, {"name": "total", "type": "numeric"}],
        metadata=ExecuteSQLQueryMetadata(
            rows_returned=1,
            is_truncated=False,
            next_page_token="page-2",
            partial_reason="PAGINATED",
        ),
    )
    execute_tool.ainvoke = AsyncMock(
        return_value=execute_payload.model_dump_json(exclude_none=True)
    )

    state = {
        "messages": [HumanMessage(content="Show recent orders with totals")],
        "active_query": "Show recent orders with totals",
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "retry_count": 0,
        "tenant_id": 1,
    }

    with (
        patch("agent.nodes.retrieve.get_mcp_tools", AsyncMock(return_value=[subgraph_tool])),
        patch("agent.tools.get_mcp_tools", AsyncMock(return_value=[recommend_tool])),
        patch("agent.nodes.execute.get_mcp_tools", AsyncMock(return_value=[execute_tool])),
        patch("agent.nodes.retrieve.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.plan.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.generate.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.validate.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.execute.telemetry.start_span", return_value=_DummySpan()),
        patch("agent.nodes.plan.ChatPromptTemplate") as mock_plan_prompt,
        patch("agent.nodes.generate.ChatPromptTemplate") as mock_generate_prompt,
        patch("agent.nodes.execute.PolicyEnforcer") as mock_enforcer,
        patch("agent.nodes.execute.TenantRewriter") as mock_rewriter,
        patch("agent.llm_client.get_llm", return_value=MagicMock()),
    ):
        _mock_prompt_response(
            mock_plan_prompt,
            json.dumps(
                {
                    "procedural_plan": ["Step 1: Use orders table"],
                    "clause_map": {"from": ["orders"]},
                    "schema_ingredients": ["orders.id", "orders.total"],
                }
            ),
        )
        _mock_prompt_response(
            mock_generate_prompt,
            "SELECT orders.id, orders.total FROM orders ORDER BY orders.id DESC",
        )
        mock_enforcer.validate_sql.return_value = None
        mock_rewriter.rewrite_sql = AsyncMock(side_effect=lambda sql, tid: sql)

        state.update(await retrieve_context_node(state))
        state.update(await plan_sql_node(state))
        state.update(await generate_sql_node(state))
        state.update(await validate_sql_node(state))
        state.update(await validate_and_execute_node(state))

    assert state["table_names"] == ["orders"]
    assert "orders" in state["schema_context"].lower()
    assert state["current_sql"].lower().startswith("select orders.id")
    assert state["ast_validation_result"]["is_valid"] is True
    assert state["query_result"] == [{"id": 1, "total": 100}]
    assert state["result_completeness"]["next_page_token"] == "page-2"
    assert state["result_completeness"]["partial_reason"] == "PAGINATED"
    assert state["result_rows_returned"] == 1

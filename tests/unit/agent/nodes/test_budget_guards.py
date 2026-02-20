"""Tests for budget/deadline guards in plan and router nodes."""

import json
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.models.termination import TerminationReason
from agent.nodes.plan import plan_sql_node
from agent.nodes.router import router_node
from common.models.error_metadata import ErrorCategory


def _mock_span_cm():
    span = MagicMock()
    span.set_inputs = MagicMock()
    span.set_outputs = MagicMock()
    span.set_attribute = MagicMock()
    span_cm = MagicMock()
    span_cm.__enter__ = MagicMock(return_value=span)
    span_cm.__exit__ = MagicMock(return_value=None)
    return span_cm


@pytest.mark.asyncio
async def test_plan_budget_exhausted_skips_llm():
    """Plan node should return early when token budget is exhausted."""
    state = {
        "messages": [HumanMessage(content="Show top customers by spend")],
        "schema_context": "Table: customers(id, spend)",
        "token_budget": {"max_tokens": 100, "consumed_tokens": 100},
    }

    with (
        patch("agent.nodes.plan.telemetry.start_span", return_value=_mock_span_cm()),
        patch("agent.llm_client.get_llm") as mock_get_llm,
    ):
        result = await plan_sql_node(state)

    assert result["termination_reason"] == TerminationReason.BUDGET_EXHAUSTED
    assert result["error_category"] == "budget_exhausted"
    mock_get_llm.assert_not_called()


@pytest.mark.asyncio
async def test_plan_deadline_exceeded_skips_llm():
    """Plan node should return early when deadline has already expired."""
    state = {
        "messages": [HumanMessage(content="Show top customers by spend")],
        "schema_context": "Table: customers(id, spend)",
        "token_budget": {"max_tokens": 100, "consumed_tokens": 10},
        "deadline_ts": time.monotonic() - 1.0,
    }

    with (
        patch("agent.nodes.plan.telemetry.start_span", return_value=_mock_span_cm()),
        patch("agent.llm_client.get_llm") as mock_get_llm,
    ):
        result = await plan_sql_node(state)

    assert result["termination_reason"] == TerminationReason.TIMEOUT
    assert result["error_category"] == ErrorCategory.TIMEOUT.value
    mock_get_llm.assert_not_called()


@pytest.mark.asyncio
async def test_router_budget_exhausted_skips_llm():
    """Router node should return early when token budget is exhausted."""
    state = {
        "messages": [HumanMessage(content="Previous"), HumanMessage(content="Follow up")],
        "schema_context": "Table: customers(id, name)",
        "token_budget": {"max_tokens": 100, "consumed_tokens": 100},
    }

    with (
        patch("agent.nodes.router.telemetry.start_span", return_value=_mock_span_cm()),
        patch("agent.llm_client.get_llm") as mock_get_llm,
    ):
        result = await router_node(state)

    assert result["termination_reason"] == TerminationReason.BUDGET_EXHAUSTED
    assert result["error_category"] == "budget_exhausted"
    mock_get_llm.assert_not_called()


@pytest.mark.asyncio
async def test_router_deadline_exceeded_skips_llm():
    """Router node should return early when deadline has already expired."""
    state = {
        "messages": [HumanMessage(content="Show sales by region")],
        "schema_context": "Table: sales(region, amount)",
        "raw_schema_context": [],
        "token_budget": {"max_tokens": 100, "consumed_tokens": 0},
        "deadline_ts": time.monotonic() - 1.0,
    }

    resolver_tool = MagicMock()
    resolver_tool.name = "resolve_ambiguity"
    resolver_tool.ainvoke = AsyncMock(
        return_value=json.dumps(
            [{"status": "AMBIGUOUS", "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE"}]
        )
    )

    with (
        patch("agent.nodes.router.telemetry.start_span", return_value=_mock_span_cm()),
        patch("agent.nodes.router.get_mcp_tools", AsyncMock(return_value=[resolver_tool])),
        patch("agent.llm_client.get_llm") as mock_get_llm,
    ):
        result = await router_node(state)

    assert result["termination_reason"] == TerminationReason.TIMEOUT
    assert result["error_category"] == ErrorCategory.TIMEOUT.value
    mock_get_llm.assert_not_called()

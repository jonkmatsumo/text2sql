"""Unit tests for SQL-of-Thought planner node."""

import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage

from agent.nodes.plan import plan_sql_node


@pytest.fixture
def base_state():
    """Create a base agent state for testing."""
    return {
        "messages": [HumanMessage(content="Show me the top 10 customers by total spend")],
        "schema_context": """
            Table: customers (customer_id, name, email, created_at)
            Table: orders (order_id, customer_id, amount, order_date)
        """,
        "table_names": ["customers", "orders"],
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": 1,
    }


def create_mock_span():
    """Create a mock span context manager."""
    mock_span_obj = MagicMock()
    mock_span_obj.set_inputs = MagicMock()
    mock_span_obj.set_outputs = MagicMock()
    mock_span_obj.set_attribute = MagicMock()

    mock_span = MagicMock()
    mock_span.__enter__ = MagicMock(return_value=mock_span_obj)
    mock_span.__exit__ = MagicMock(return_value=None)
    return mock_span


class TestPlanSqlNode:
    """Tests for plan_sql_node function."""

    @pytest.mark.asyncio
    async def test_plan_generation_basic(self, base_state):
        """Test basic plan generation."""
        plan_json = json.dumps(
            {
                "schema_linking": {
                    "relevant_tables": ["customers", "orders"],
                    "relevant_columns": ["customers.customer_id", "orders.amount"],
                },
                "procedural_plan": [
                    "Step 1: Join customers with orders on customer_id",
                    "Step 2: Sum the order amounts per customer",
                    "Step 3: Order by total descending",
                    "Step 4: Limit to 10 results",
                ],
                "clause_map": {
                    "from": ["customers"],
                    "joins": [{"type": "JOIN", "table": "orders", "on": "customer_id"}],
                    "group_by": ["customers.customer_id"],
                    "order_by": ["total DESC"],
                    "limit": 10,
                },
                "schema_ingredients": [
                    "customers.customer_id",
                    "orders.amount",
                ],
            }
        )

        mock_response = MagicMock()
        mock_response.content = plan_json

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent.nodes.plan.ChatPromptTemplate") as mock_prompt,
            patch("agent.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await plan_sql_node(base_state)

        assert result.get("procedural_plan") is not None
        assert "Step 1" in result["procedural_plan"]

    @pytest.mark.asyncio
    async def test_plan_with_empty_query(self, base_state):
        """Test plan generation with empty query."""
        base_state["messages"] = []

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent.llm_client.get_llm"),
        ):
            result = await plan_sql_node(base_state)

        # Should return empty dict for empty query
        assert result == {}

    @pytest.mark.asyncio
    async def test_plan_with_clarification(self, base_state):
        """Test plan includes user clarification when available."""
        base_state["user_clarification"] = "I mean by total revenue"

        plan_json = json.dumps(
            {
                "procedural_plan": ["Step 1: Consider total revenue as the metric"],
                "clause_map": {},
                "schema_ingredients": [],
            }
        )

        mock_response = MagicMock()
        mock_response.content = plan_json

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent.nodes.plan.ChatPromptTemplate") as mock_prompt,
            patch("agent.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await plan_sql_node(base_state)

        # Plan should be generated
        assert result.get("procedural_plan") is not None

    @pytest.mark.asyncio
    async def test_plan_json_parse_fallback(self, base_state):
        """Test fallback when JSON parsing fails."""
        mock_response = MagicMock()
        mock_response.content = "This is not valid JSON - just raw text plan"

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value=mock_response)

        with (
            patch("mlflow.start_span", return_value=create_mock_span()),
            patch("agent.nodes.plan.ChatPromptTemplate") as mock_prompt,
            patch("agent.llm_client.get_llm"),
        ):
            mock_prompt.from_messages.return_value.__or__ = MagicMock(return_value=mock_chain)

            result = await plan_sql_node(base_state)

        # Should still return a procedural plan (using raw text as fallback)
        assert result.get("procedural_plan") is not None

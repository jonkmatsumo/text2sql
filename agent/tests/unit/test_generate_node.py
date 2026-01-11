"""Unit tests for generate_sql_node."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage


class TestGenerateSqlNode:
    """Tests for generate_sql_node function."""

    @pytest.fixture
    def mock_state(self):
        """Create a mock agent state with schema context from retrieve node."""
        return {
            "messages": [HumanMessage(content="Show me all orders")],
            "active_query": "Show me all orders",
            "schema_context": "## Table: Orders\n\n- **order_id** (integer): Primary key",
            "table_names": ["Orders"],
            "tenant_id": 1,
        }

    @pytest.mark.asyncio
    async def test_generate_sql_node_does_not_call_schema_tool(self, mock_state):
        """Verify get_table_schema is NOT called since we removed that code."""
        from agent_core.nodes.generate import generate_sql_node

        # Mock the schema tool
        mock_schema_tool = MagicMock()
        mock_schema_tool.name = "get_table_schema"
        mock_schema_tool.ainvoke = AsyncMock(return_value="[]")

        # Track all tool calls
        mock_tools = [mock_schema_tool]

        with patch("agent_core.tools.get_mcp_tools", new_callable=AsyncMock) as mock_get_tools:
            with patch("agent_core.nodes.generate.mlflow") as mock_mlflow:
                with patch("agent_core.nodes.generate.llm") as mock_llm:
                    # Setup mlflow span mock
                    mock_span = MagicMock()
                    mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
                    mock_mlflow.entities.SpanType.CHAT_MODEL = "CHAT_MODEL"

                    mock_get_tools.return_value = mock_tools

                    # Mock the LLM chain - return an object with .content attribute
                    mock_response = MagicMock()
                    mock_response.content = "SELECT * FROM orders LIMIT 1000"

                    # Mock prompt | llm chain
                    mock_chain = MagicMock()
                    mock_chain.invoke.return_value = mock_response

                    # When ChatPromptTemplate | llm is called
                    def mock_or(other):
                        return mock_chain

                    mock_llm.__or__ = mock_or

                    await generate_sql_node(mock_state)

                    # CRUCIAL: Verify get_table_schema was NEVER called
                    mock_schema_tool.ainvoke.assert_not_called()

"""Unit tests for retrieve_context_node."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import HumanMessage


class TestRetrieveContextNode:
    """Tests for retrieve_context_node function."""

    @pytest.fixture
    def sample_graph_data(self):
        """Sample graph data mimicking get_semantic_subgraph output."""
        return {
            "nodes": [
                {
                    "id": "t1",
                    "name": "Users",
                    "type": "Table",
                    "description": "User accounts",
                },
                {
                    "id": "t2",
                    "name": "Orders",
                    "type": "Table",
                    "description": "Customer orders",
                },
                {
                    "id": "c1",
                    "name": "user_id",
                    "type": "Column",
                    "data_type": "integer",
                },
                {
                    "id": "c2",
                    "name": "order_id",
                    "type": "Column",
                    "data_type": "integer",
                },
            ],
            "relationships": [
                {"source": "t1", "target": "c1", "type": "HAS_COLUMN"},
                {"source": "t2", "target": "c2", "type": "HAS_COLUMN"},
                {"source": "c1", "target": "t2", "type": "FOREIGN_KEY_TO"},
            ],
        }

    @pytest.fixture
    def mock_state(self):
        """Create a mock agent state."""
        return {
            "messages": [HumanMessage(content="Show me all orders")],
            "active_query": "Show me all orders",
        }

    @pytest.mark.asyncio
    async def test_retrieve_context_node_success(self, sample_graph_data, mock_state):
        """Test successful context retrieval with semantic subgraph tool."""
        from agent_core.nodes.retrieve import retrieve_context_node

        # Mock the subgraph tool
        mock_tool = MagicMock()
        mock_tool.name = "get_semantic_subgraph"
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps(sample_graph_data))

        with patch(
            "agent_core.nodes.retrieve.get_mcp_tools", new_callable=AsyncMock
        ) as mock_get_tools:
            with patch("agent_core.nodes.retrieve.telemetry") as mock_mlflow:
                # Setup mock for telemetry span
                mock_span = MagicMock()
                mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
                mock_mlflow.entities.SpanType.RETRIEVER = "RETRIEVER"

                mock_get_tools.return_value = [mock_tool]

                result = await retrieve_context_node(mock_state)

                # Assertions
                mock_tool.ainvoke.assert_called_once_with({"query": "Show me all orders"})
                assert isinstance(result, dict)
                assert "schema_context" in result
                assert "table_names" in result

                # Verify schema_context contains compact table format
                assert "**Users**" in result["schema_context"]
                assert "**Orders**" in result["schema_context"]

                # Verify table_names contains expected tables
                assert isinstance(result["table_names"], list)
                assert "Users" in result["table_names"]
                assert "Orders" in result["table_names"]

    @pytest.mark.asyncio
    async def test_retrieve_context_node_empty_graph(self, mock_state):
        """Test retrieval when graph returns no nodes."""
        from agent_core.nodes.retrieve import retrieve_context_node

        empty_graph = {"nodes": [], "relationships": []}

        mock_tool = MagicMock()
        mock_tool.name = "get_semantic_subgraph"
        mock_tool.ainvoke = AsyncMock(return_value=json.dumps(empty_graph))

        with patch(
            "agent_core.nodes.retrieve.get_mcp_tools", new_callable=AsyncMock
        ) as mock_get_tools:
            with patch("agent_core.nodes.retrieve.telemetry") as mock_mlflow:
                mock_span = MagicMock()
                mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
                mock_mlflow.entities.SpanType.RETRIEVER = "RETRIEVER"

                mock_get_tools.return_value = [mock_tool]

                result = await retrieve_context_node(mock_state)

                assert result["schema_context"] == "No relevant tables found."
                assert result["table_names"] == []

    @pytest.mark.asyncio
    async def test_retrieve_context_node_tool_not_found(self, mock_state):
        """Test handling when subgraph tool is not available."""
        from agent_core.nodes.retrieve import retrieve_context_node

        with patch(
            "agent_core.nodes.retrieve.get_mcp_tools", new_callable=AsyncMock
        ) as mock_get_tools:
            with patch("agent_core.nodes.retrieve.telemetry") as mock_mlflow:
                mock_span = MagicMock()
                mock_mlflow.start_span.return_value.__enter__.return_value = mock_span
                mock_mlflow.entities.SpanType.RETRIEVER = "RETRIEVER"

                mock_get_tools.return_value = []  # No tools available

                result = await retrieve_context_node(mock_state)

                assert "not available" in result["schema_context"]
                assert result["table_names"] == []

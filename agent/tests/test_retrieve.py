"""Unit tests for context retrieval node."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from agent_core.nodes.retrieve import retrieve_context_node
from agent_core.state import AgentState


class TestRetrieveContextNode:
    """Unit tests for retrieve_context_node function."""

    def _mock_mlflow_span(self, mock_start_span):
        """Mock the MLflow span context manager."""
        mock_span = MagicMock()
        mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
        return mock_span

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_success(self, mock_get_mcp_tools, mock_start_span):
        """Test successful context retrieval."""
        self._mock_mlflow_span(mock_start_span)

        # Create mock tool
        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"

        import json

        mock_response = [
            {
                "table_name": "customer",
                "description": "Customer table",
                "columns": [
                    {"name": "id", "type": "integer", "required": True},
                    {"name": "name", "type": "text", "required": True},
                ],
            },
            {
                "table_name": "payment",
                "description": "Payment table",
                "columns": [
                    {"name": "amount", "type": "numeric", "required": True},
                    {"name": "customer_id", "type": "integer", "required": True},
                ],
            },
        ]
        mock_search_tool.ainvoke = AsyncMock(return_value=json.dumps(mock_response))
        mock_get_mcp_tools.return_value = [mock_search_tool]

        # Create test state
        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="Show me customer payments")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify MCP tools were called
        mock_get_mcp_tools.assert_called_once()

        # Verify search tool was invoked with correct query and limit
        mock_search_tool.ainvoke.assert_called_once_with(
            {"user_query": "Show me customer payments", "limit": 5}
        )

        # Verify context was returned correctly
        assert "schema_context" in result
        # Check for new format: "Table: customer... Description: Customer table... Columns:..."
        assert "Table: customer" in result["schema_context"]
        assert "Description: Customer table" in result["schema_context"]
        assert "- id (integer, REQUIRED)" in result["schema_context"]

        assert "table_names" in result
        assert "customer" in result["table_names"]
        assert "payment" in result["table_names"]

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_extracts_query(self, mock_get_mcp_tools, mock_start_span):
        """Test that query is extracted from last message."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"
        mock_search_tool.ainvoke = AsyncMock(return_value="")
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        test_query = "Find all actors in action movies"
        state = AgentState(
            messages=[HumanMessage(content=test_query)],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        await retrieve_context_node(state)

        # Verify search tool was called with the extracted query
        mock_search_tool.ainvoke.assert_called_once_with({"user_query": test_query, "limit": 5})

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_top_k(self, mock_get_mcp_tools, mock_start_span):
        """Test that limit=5 is used for similarity search."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"
        mock_search_tool.ainvoke = AsyncMock(return_value="")
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        await retrieve_context_node(state)

        # Verify limit=5 was used
        mock_search_tool.ainvoke.assert_called_once_with({"user_query": "test query", "limit": 5})

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_formatting(self, mock_get_mcp_tools, mock_start_span):
        """Test context string formatting."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"

        mock_data = [
            {"table_name": "table1", "description": "Schema 1", "columns": []},
            {"table_name": "table2", "description": "Schema 2", "columns": []},
            {"table_name": "table3", "description": "Schema 3", "columns": []},
        ]

        mock_search_tool.ainvoke = AsyncMock(return_value=mock_data)
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify schema context matches the response
        assert "Table: table1" in result["schema_context"]
        assert "Table: table2" in result["schema_context"]
        assert "Table: table3" in result["schema_context"]

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_empty_results(self, mock_get_mcp_tools, mock_start_span):
        """Test handling of empty search results."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"
        mock_search_tool.ainvoke = AsyncMock(return_value=[])
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify empty context string is returned
        assert "No relevant tables found" in result["schema_context"]
        assert result["table_names"] == []

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_multiple_messages(
        self, mock_get_mcp_tools, mock_start_span
    ):
        """Test that query is extracted from the last message when multiple messages exist."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"
        mock_search_tool.ainvoke = AsyncMock(return_value="")
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import AIMessage, HumanMessage

        # Create state with multiple messages
        state = AgentState(
            messages=[
                HumanMessage(content="First query"),
                AIMessage(content="Response"),
                HumanMessage(content="Second query"),
            ],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        await retrieve_context_node(state)

        # Verify last message content was used
        mock_search_tool.ainvoke.assert_called_once_with({"user_query": "Second query", "limit": 5})

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_error_handling(self, mock_get_mcp_tools, mock_start_span):
        """Test error handling when MCP tools raise an exception."""
        self._mock_mlflow_span(mock_start_span)

        mock_get_mcp_tools.side_effect = Exception("MCP connection error")

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Verify error is handled gracefully (returns error message in context)
        result = await retrieve_context_node(state)
        assert "Error retrieving context" in result["schema_context"]

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_single_result(self, mock_get_mcp_tools, mock_start_span):
        """Test context retrieval with a single result."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"

        # Test unwrapping nested message-like structure (User scenario)
        class MockMessage:
            def __init__(self, text):
                self.text = text

        json_payload = (
            '[{"table_name": "single_table", "description": "Single schema", "columns": []}]'
        )
        mock_response = [MockMessage(text=json_payload)]

        mock_search_tool.ainvoke = AsyncMock(return_value=mock_response)
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify single result is returned
        assert "Single schema" in result["schema_context"]
        assert "single_table" in result["table_names"]

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_max_results(self, mock_get_mcp_tools, mock_start_span):
        """Test that limit=5 is passed to search tool."""
        self._mock_mlflow_span(mock_start_span)

        mock_search_tool = MagicMock()
        mock_search_tool.name = "search_relevant_tables_tool"
        # Create 5 mock table results
        import json

        mock_rows = [
            {"table_name": f"table{i+1}", "description": f"Schema {i+1}"} for i in range(5)
        ]
        mock_search_tool.ainvoke = AsyncMock(return_value=json.dumps(mock_rows))
        mock_get_mcp_tools.return_value = [mock_search_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify all 5 results are included
        for i in range(5):
            assert f"Schema {i + 1}" in result["schema_context"]
            assert f"table{i + 1}" in result["table_names"]

    @pytest.mark.asyncio
    @patch("agent_core.nodes.retrieve.mlflow.start_span")
    @patch("agent_core.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_tool_not_found(self, mock_get_mcp_tools, mock_start_span):
        """Test handling when search tool is not found."""
        self._mock_mlflow_span(mock_start_span)

        # Return tools without the search tool
        mock_other_tool = MagicMock()
        mock_other_tool.name = "other_tool"
        mock_get_mcp_tools.return_value = [mock_other_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        result = await retrieve_context_node(state)

        # Verify empty context is returned when tool not found
        assert result["schema_context"] == ""
        assert result["table_names"] == []

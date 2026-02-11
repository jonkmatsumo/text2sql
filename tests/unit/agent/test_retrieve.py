"""Unit tests for context retrieval node."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.nodes.retrieve import retrieve_context_node
from agent.state import AgentState


class TestRetrieveContextNode:
    """Unit tests for retrieve_context_node function."""

    def _mock_telemetry_span(self, mock_start_span):
        """Mock the telemetry span context manager."""
        mock_span = MagicMock()
        mock_start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_start_span.return_value.__exit__ = MagicMock(return_value=False)
        return mock_span

    def _make_graph_data(self, tables, columns=None, relationships=None):
        """Build graph data structure matching get_semantic_subgraph output."""
        nodes = []
        for i, table in enumerate(tables):
            nodes.append(
                {
                    "id": f"t{i}",
                    "name": table["name"],
                    "type": "Table",
                    "description": table.get("description", ""),
                }
            )
        if columns:
            for i, col in enumerate(columns):
                nodes.append(
                    {
                        "id": f"c{i}",
                        "name": col["name"],
                        "type": "Column",
                        "data_type": col.get("type", "text"),
                        "table": col.get("table", ""),
                    }
                )
        return {
            "nodes": nodes,
            "relationships": relationships or [],
        }

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_success(self, mock_get_mcp_tools, mock_start_span):
        """Test successful context retrieval."""
        self._mock_telemetry_span(mock_start_span)

        # Create mock tool matching get_semantic_subgraph
        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"

        graph_data = self._make_graph_data(
            tables=[
                {"name": "customer", "description": "Customer table"},
                {"name": "payment", "description": "Payment table"},
            ],
            columns=[
                {"name": "id", "type": "integer", "table": "customer"},
                {"name": "name", "type": "text", "table": "customer"},
                {"name": "amount", "type": "numeric", "table": "payment"},
            ],
            relationships=[
                {"source": "t0", "target": "c0", "type": "HAS_COLUMN"},
                {"source": "t0", "target": "c1", "type": "HAS_COLUMN"},
                {"source": "t1", "target": "c2", "type": "HAS_COLUMN"},
            ],
        )

        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(graph_data))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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

        # Verify subgraph tool was invoked with query
        mock_subgraph_tool.ainvoke.assert_called_once_with({"query": "Show me customer payments"})

        # Verify context was returned (uses compact markdown format)
        assert "schema_context" in result
        assert "customer" in result["schema_context"]
        assert "payment" in result["schema_context"]

        assert "table_names" in result
        assert "customer" in result["table_names"]
        assert "payment" in result["table_names"]
        assert result["schema_snapshot_id"].startswith("fp-")

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_extracts_query(self, mock_get_mcp_tools, mock_start_span):
        """Test that query is extracted from last message."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        mock_subgraph_tool.ainvoke = AsyncMock(
            return_value=json.dumps({"nodes": [], "relationships": []})
        )
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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

        # Verify subgraph tool was called with the extracted query
        mock_subgraph_tool.ainvoke.assert_called_once_with({"query": test_query})

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_formatting(self, mock_get_mcp_tools, mock_start_span):
        """Test context string formatting."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"

        graph_data = self._make_graph_data(
            tables=[
                {"name": "table1", "description": "Schema 1"},
                {"name": "table2", "description": "Schema 2"},
                {"name": "table3", "description": "Schema 3"},
            ]
        )

        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(graph_data))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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

        # Verify schema context uses compact format with **table**
        assert "**table1**" in result["schema_context"]
        assert "**table2**" in result["schema_context"]
        assert "**table3**" in result["schema_context"]

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_empty_results(self, mock_get_mcp_tools, mock_start_span):
        """Test handling of empty search results."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        mock_subgraph_tool.ainvoke = AsyncMock(
            return_value=json.dumps({"nodes": [], "relationships": []})
        )
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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
        assert "No relevant tables found." in result["schema_context"]
        assert result["table_names"] == []

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_multiple_messages(
        self, mock_get_mcp_tools, mock_start_span
    ):
        """Test that query is extracted from the last message when multiple messages exist."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        mock_subgraph_tool.ainvoke = AsyncMock(
            return_value=json.dumps({"nodes": [], "relationships": []})
        )
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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
        mock_subgraph_tool.ainvoke.assert_called_once_with({"query": "Second query"})

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_error_handling(self, mock_get_mcp_tools, mock_start_span):
        """Test error handling when MCP tools raise an exception."""
        self._mock_telemetry_span(mock_start_span)

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
        assert "Error in context retrieval" in result["schema_context"]

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_no_prints_and_safe_logs(
        self, mock_get_mcp_tools, mock_start_span
    ):
        """Ensure retrieve does not print and logs only safe metadata."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        mock_subgraph_tool.ainvoke = AsyncMock(
            return_value=json.dumps({"nodes": [], "relationships": []})
        )
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="test query")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
            interaction_id="interaction-123",
        )

        with (
            patch("builtins.print") as mock_print,
            patch("agent.nodes.retrieve.logger") as mock_logger,
        ):
            await retrieve_context_node(state)

            mock_print.assert_not_called()
            assert mock_logger.info.called
            extra = mock_logger.info.call_args.kwargs.get("extra", {})
            allowed_keys = {
                "interaction_id",
                "schema_source",
                "tables_retrieved",
                "nodes_retrieved",
                "snapshot_id",
            }
            assert set(extra.keys()).issubset(allowed_keys)

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_single_result(self, mock_get_mcp_tools, mock_start_span):
        """Test context retrieval with a single result."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"

        graph_data = self._make_graph_data(
            tables=[{"name": "single_table", "description": "Single schema"}]
        )

        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(graph_data))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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
        assert "single_table" in result["schema_context"]
        assert "single_table" in result["table_names"]

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_max_results(self, mock_get_mcp_tools, mock_start_span):
        """Test retrieval with multiple tables."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"

        # Create 5 tables
        tables = [{"name": f"table{i+1}", "description": f"Schema {i+1}"} for i in range(5)]
        graph_data = self._make_graph_data(tables=tables)

        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(graph_data))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

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
            assert f"table{i + 1}" in result["schema_context"]
            assert f"table{i + 1}" in result["table_names"]

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_retrieve_context_node_tool_not_found(self, mock_get_mcp_tools, mock_start_span):
        """Test handling when subgraph tool is not found."""
        self._mock_telemetry_span(mock_start_span)

        # Return tools without the subgraph tool
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

        # Verify error message when tool not found
        assert "not available" in result["schema_context"]
        assert result["table_names"] == []

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_schema_fingerprint_pinned_across_retrieve_calls(
        self, mock_get_mcp_tools, mock_start_span
    ):
        """Schema fingerprint should be set once and remain stable in normal retrieval."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        graph_data = self._make_graph_data(
            tables=[{"name": "orders", "description": "Orders table"}],
            columns=[{"name": "order_id", "type": "int", "table": "orders"}],
        )
        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(graph_data))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="show orders")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        first = await retrieve_context_node(state)
        state.update(first)
        second = await retrieve_context_node(state)

        assert first["schema_fingerprint"]
        assert second["schema_fingerprint"] == first["schema_fingerprint"]
        assert second["schema_version_ts"] == first["schema_version_ts"]

    @pytest.mark.asyncio
    @patch("agent.nodes.retrieve.telemetry.start_span")
    @patch("agent.nodes.retrieve.get_mcp_tools")
    async def test_schema_fingerprint_changes_only_on_refresh_path(
        self, mock_get_mcp_tools, mock_start_span
    ):
        """Fingerprint should change only when a schema-refresh transition is triggered."""
        self._mock_telemetry_span(mock_start_span)

        mock_subgraph_tool = MagicMock()
        mock_subgraph_tool.name = "get_semantic_subgraph"
        new_graph = self._make_graph_data(
            tables=[{"name": "orders_v2", "description": "Orders v2"}],
            columns=[{"name": "order_id", "type": "int", "table": "orders_v2"}],
        )
        mock_subgraph_tool.ainvoke = AsyncMock(return_value=json.dumps(new_graph))
        mock_get_mcp_tools.return_value = [mock_subgraph_tool]

        from langchain_core.messages import HumanMessage

        state = AgentState(
            messages=[HumanMessage(content="show orders")],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
            schema_snapshot_id="fp-old",
            schema_fingerprint="oldfingerprint",
            schema_version_ts=123,
            schema_refresh_count=0,
        )

        refresh_transition = await retrieve_context_node(state)
        assert refresh_transition["schema_refresh_count"] == 1
        assert refresh_transition["error"] == "Schema drift detected"
        assert refresh_transition["schema_fingerprint"] != state["schema_fingerprint"]

        state.update(refresh_transition)
        stable_after_refresh = await retrieve_context_node(state)
        assert (
            stable_after_refresh["schema_fingerprint"] == refresh_transition["schema_fingerprint"]
        )

"""Unit tests for schema-aware clarification flow.

These tests verify that the clarification node receives schema context
before making ambiguity decisions, preventing hallucinated questions.
"""

from unittest.mock import MagicMock, patch

from agent_core.graph import create_workflow, route_after_router


class TestSchemaAwareClarification:
    """Tests verifying schema context is available to router/clarify nodes."""

    def test_workflow_entry_point_is_retrieve(self):
        """Verify workflow starts with retrieve to populate schema context."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Entry point should be retrieve, not router
            mock_workflow.set_entry_point.assert_called_once_with("retrieve")

    def test_retrieve_feeds_router(self):
        """Verify retrieve node output flows to router node."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Should have edge from retrieve to router
            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]
            assert ("retrieve", "router") in edge_calls

    def test_router_routes_to_plan_not_retrieve(self):
        """Verify router goes to plan (not retrieve) when no ambiguity."""
        # State with schema context (already retrieved)
        state = {
            "messages": [],
            "ambiguity_type": None,
            "schema_context": "## Tables\n- film\n- customer",
            "table_names": ["film", "customer"],
        }

        result = route_after_router(state)

        # Should go to plan since retrieve already happened
        assert result == "plan"

    def test_router_routes_to_clarify_when_ambiguous(self):
        """Verify router still routes to clarify when ambiguity detected."""
        state = {
            "messages": [],
            "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
            "clarification_question": "Do you mean region from customers or stores?",
            "schema_context": "## Tables\n- customer (region column)\n- store (region column)",
        }

        result = route_after_router(state)

        assert result == "clarify"

    def test_clarify_loops_to_router_not_retrieve(self):
        """Verify clarify loops back to router (schema is still valid)."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Clarify should loop back to router
            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]
            assert ("clarify", "router") in edge_calls

            # But clarify should NOT go to retrieve
            assert ("clarify", "retrieve") not in edge_calls


class TestRouterConditionalEdges:
    """Tests for router conditional edge configuration."""

    def test_router_conditional_edges_include_plan(self):
        """Verify router conditional edges map to plan (not retrieve)."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Find the router conditional edge call
            conditional_calls = mock_workflow.add_conditional_edges.call_args_list
            router_call = None
            for call in conditional_calls:
                if call[0][0] == "router":
                    router_call = call
                    break

            assert router_call is not None
            # Third argument is the routing map
            routing_map = router_call[0][2]
            assert "clarify" in routing_map
            assert "plan" in routing_map
            # Should NOT contain "retrieve" anymore
            assert "retrieve" not in routing_map


class TestFlowIntegration:
    """Integration tests for the complete flow order."""

    def test_complete_flow_order(self):
        """Test that the expected flow order is maintained."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Collect all edges
            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]

            # Expected direct edges in new flow:
            # retrieve -> router
            # clarify -> router (loop)
            # plan -> generate
            # generate -> validate
            # correct -> validate (loop)
            # synthesize -> END

            assert ("retrieve", "router") in edge_calls
            assert ("clarify", "router") in edge_calls
            assert ("plan", "generate") in edge_calls
            assert ("generate", "validate") in edge_calls
            assert ("correct", "validate") in edge_calls

    def test_no_direct_router_to_retrieve_edge(self):
        """Ensure there's no edge from router directly to retrieve."""
        with patch("agent_core.graph.StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]

            # Router should NOT go to retrieve (it goes to plan now)
            assert ("router", "retrieve") not in edge_calls


class TestSchemaContextScenarios:
    """Tests simulating realistic schema context scenarios."""

    def test_single_column_should_not_trigger_clarification(self):
        """If only one 'runtime' column exists, no clarification needed.

        This is the key scenario: the old flow would ask about runtime
        variations even when only one column exists because it had no
        schema context.
        """
        # State after retrieve has populated schema
        state = {
            "messages": [{"content": "What is the average runtime?"}],
            "ambiguity_type": None,  # Router should NOT set this with schema
            "schema_context": """## Tables
### film
| Column | Type |
|--------|------|
| film_id | integer |
| title | varchar |
| runtime | integer |  <-- Only ONE runtime column
""",
            "table_names": ["film"],
        }

        result = route_after_router(state)

        # Should proceed to plan, not clarify
        assert result == "plan"

    def test_multiple_ambiguous_columns_may_trigger_clarification(self):
        """If 'region' exists in multiple tables, clarification may be needed."""
        state = {
            "messages": [{"content": "Show sales by region"}],
            "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",  # Router detected ambiguity
            "clarification_question": "Do you mean region from customers or stores?",
            "schema_context": """## Tables
### customer
| Column | Type |
|--------|------|
| customer_id | integer |
| region | varchar |  <-- ambiguous

### store
| Column | Type |
|--------|------|
| store_id | integer |
| region | varchar |  <-- ambiguous
""",
            "table_names": ["customer", "store"],
        }

        result = route_after_router(state)

        # Should go to clarify since there's genuine ambiguity
        assert result == "clarify"

    def test_empty_schema_context_still_routes_correctly(self):
        """Even if retrieve returns empty, routing should work."""
        state = {
            "messages": [],
            "ambiguity_type": None,
            "schema_context": "",  # Empty (no tables found)
            "table_names": [],
        }

        result = route_after_router(state)

        # Should still proceed (plan will handle empty context)
        assert result == "plan"

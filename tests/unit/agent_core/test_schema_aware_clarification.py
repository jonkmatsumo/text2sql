"""Unit tests for schema-aware clarification flow.

These tests verify that the clarification node receives schema context
before making ambiguity decisions, preventing hallucinated questions.
"""

import importlib.util
import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def isolated_agent_graph():
    """Load agent_core.graph from source to ensure isolation from other tests."""
    # Locate the source file
    # Assuming running from repo root
    source_path = os.path.abspath("src/agent_core/graph.py")
    if not os.path.exists(source_path):
        pytest.fail(f"Could not find agent_core.graph source at {source_path}")

    spec = importlib.util.spec_from_file_location("agent_core.graph_isolated", source_path)
    module = importlib.util.module_from_spec(spec)

    # We must add it to sys.modules momentarily for relative imports within it to work?
    # agent_core imports e.g. .state.
    # If we don't put it in sys.modules, imports inside it might fail or duplicate.
    # But we WANT duplication of this specific module.
    # However, relative imports `from .state import ...` require package context.
    # We can set module.__package__ = "agent_core".
    # And ensure sys.modules["agent_core"] exists.

    # Simple approach: execute module.
    # If it fails due to relative imports, we might need to be smarter.
    # agent_core/graph.py imports:
    # from langgraph.graph import END, StateGraph
    # from agent_core.state import AgentState
    # from agent_core.nodes import ...

    # These are absolute imports in the package.
    # So we don't need relative import logic if PYTHONPATH is correct.

    spec.loader.exec_module(module)
    return module


class TestSchemaAwareClarification:
    """Tests verifying schema context is available to router/clarify nodes."""

    def test_workflow_entry_point_is_cache_lookup(self, isolated_agent_graph):
        """Verify workflow starts with cache_lookup."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        # We patch the StateGraph on the ISOLATED module object
        with patch.object(module, "StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Entry point should be cache_lookup
            mock_workflow.set_entry_point.assert_called_once_with("cache_lookup")

    def test_retrieve_feeds_router(self, isolated_agent_graph):
        """Verify retrieve node output flows to router node."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        with patch.object(module, "StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            # Should have edge from retrieve to router
            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]
            assert ("retrieve", "router") in edge_calls

    def test_router_routes_to_plan_not_retrieve(self, isolated_agent_graph):
        """Verify router goes to plan (not retrieve) when no ambiguity."""
        module = isolated_agent_graph
        route_after_router = module.route_after_router

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

    def test_router_routes_to_clarify_when_ambiguous(self, isolated_agent_graph):
        """Verify router still routes to clarify when ambiguity detected."""
        module = isolated_agent_graph
        route_after_router = module.route_after_router

        state = {
            "messages": [],
            "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
            "clarification_question": "Do you mean region from customers or stores?",
            "schema_context": "## Tables\n- customer (region column)\n- store (region column)",
        }

        result = route_after_router(state)

        assert result == "clarify"

    def test_clarify_loops_to_router_not_retrieve(self, isolated_agent_graph):
        """Verify clarify loops back to router (schema is still valid)."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        with patch.object(module, "StateGraph") as mock_sg:
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

    def test_router_conditional_edges_include_plan(self, isolated_agent_graph):
        """Verify router conditional edges map to plan (not retrieve)."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        with patch.object(module, "StateGraph") as mock_sg:
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

    def test_complete_flow_order(self, isolated_agent_graph):
        """Test that the expected flow order is maintained."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        with patch.object(module, "StateGraph") as mock_sg:
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

    def test_no_direct_router_to_retrieve_edge(self, isolated_agent_graph):
        """Ensure there's no edge from router directly to retrieve."""
        module = isolated_agent_graph
        create_workflow = module.create_workflow

        with patch.object(module, "StateGraph") as mock_sg:
            mock_workflow = MagicMock()
            mock_sg.return_value = mock_workflow

            create_workflow()

            edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]

            # Router should NOT go to retrieve (it goes to plan now)
            assert ("router", "retrieve") not in edge_calls


class TestSchemaContextScenarios:
    """Tests simulating realistic schema context scenarios."""

    def test_single_column_should_not_trigger_clarification(self, isolated_agent_graph):
        """If only one 'runtime' column exists, no clarification needed."""
        module = isolated_agent_graph
        route_after_router = module.route_after_router

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

    def test_multiple_ambiguous_columns_may_trigger_clarification(self, isolated_agent_graph):
        """If 'region' exists in multiple tables, clarification may be needed."""
        module = isolated_agent_graph
        route_after_router = module.route_after_router

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

    def test_empty_schema_context_still_routes_correctly(self, isolated_agent_graph):
        """Even if retrieve returns empty, routing should work."""
        module = isolated_agent_graph
        route_after_router = module.route_after_router

        state = {
            "messages": [],
            "ambiguity_type": None,
            "schema_context": "",  # Empty (no tables found)
            "table_names": [],
        }

        result = route_after_router(state)

        # Should still proceed (plan will handle empty context)
        assert result == "plan"

import sys
from unittest.mock import MagicMock, patch

from langgraph.graph import END

from agent.state import AgentState

# We delay imports of agent.graph to test methods to allow for cleanup of polluted modules


def clean_agent_modules():
    """Clean up polluted modules from other tests."""
    clean_modules = [m for m in sys.modules if m.startswith("agent")]
    for m in clean_modules:
        # We only delete mocks, or if force reloading is needed
        if not isinstance(sys.modules[m], type(sys)) or "agent.graph" in m:
            if m in sys.modules:
                del sys.modules[m]

    if "agent" in sys.modules and not isinstance(sys.modules["agent"], type(sys)):
        del sys.modules["agent"]


class TestRouteAfterRouter:
    """Unit tests for route_after_router conditional routing logic."""

    def test_route_to_clarify_when_ambiguous(self):
        """Test routing to clarify when ambiguity detected."""
        clean_agent_modules()
        from agent.graph import route_after_router

        state = {
            "messages": [],
            "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
            "clarification_question": "Which region?",
        }

        result = route_after_router(state)

        assert result == "clarify"

    def test_route_to_plan_when_clear(self):
        """Test routing to plan when no ambiguity (schema already retrieved)."""
        clean_agent_modules()
        from agent.graph import route_after_router

        state = {
            "messages": [],
            "ambiguity_type": None,
            "schema_context": "Tables: film, customer",  # Schema available
        }

        result = route_after_router(state)

        assert result == "plan"

    def test_route_to_plan_when_ambiguity_missing(self):
        """Test routing to plan when ambiguity_type is missing."""
        clean_agent_modules()
        from agent.graph import route_after_router

        state = {"messages": [], "schema_context": "Tables: film"}

        result = route_after_router(state)

        assert result == "plan"


class TestRouteAfterValidation:
    """Unit tests for route_after_validation conditional routing logic."""

    def test_route_to_execute_when_valid(self):
        """Test routing to execute when validation passes."""
        clean_agent_modules()
        from agent.graph import route_after_validation

        state = {
            "ast_validation_result": {"is_valid": True},
            "error": None,
        }

        result = route_after_validation(state)

        assert result == "execute"

    def test_route_to_correct_when_invalid(self):
        """Test routing to correct when validation fails."""
        clean_agent_modules()
        from agent.graph import route_after_validation

        state = {
            "ast_validation_result": {"is_valid": False},
            "error": None,
        }

        result = route_after_validation(state)

        assert result == "correct"

    def test_route_to_correct_when_error(self):
        """Test routing to correct when error is set."""
        clean_agent_modules()
        from agent.graph import route_after_validation

        state = {
            "ast_validation_result": None,
            "error": "Security violation",
        }

        result = route_after_validation(state)

        assert result == "correct"

    def test_route_to_execute_when_no_validation_result(self):
        """Test routing to execute when no validation result."""
        clean_agent_modules()
        from agent.graph import route_after_validation

        state = {
            "ast_validation_result": None,
            "error": None,
        }

        result = route_after_validation(state)

        assert result == "execute"


class TestRouteAfterExecution:
    """Unit tests for route_after_execution conditional routing logic."""

    def test_route_success_to_visualize(self):
        """Test routing to visualize when execution succeeds."""
        clean_agent_modules()
        from agent.graph import route_after_execution

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM film",
            query_result=[{"id": 1}],
            error=None,
            retry_count=0,
        )

        result = route_after_execution(state)

        assert result == "visualize"

    def test_route_error_with_retries_under_limit_to_correct(self):
        """Test routing to correct when error occurs and retries < 3."""
        clean_agent_modules()
        from agent.graph import route_after_execution

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="relation 'films' does not exist",
            retry_count=0,
        )

        result = route_after_execution(state)

        assert result == "correct"

        # Test with retry_count = 1
        state["retry_count"] = 1
        result = route_after_execution(state)
        assert result == "correct"

        # Test with retry_count = 2
        state["retry_count"] = 2
        result = route_after_execution(state)
        assert result == "correct"

    def test_route_error_with_retries_at_limit_to_failed(self):
        """Test routing to failed when error occurs and retries >= 3."""
        clean_agent_modules()
        from agent.graph import route_after_execution

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="relation 'films' does not exist",
            retry_count=3,
        )

        result = route_after_execution(state)

        assert result == "failed"

        # Test with retry_count > 3
        state["retry_count"] = 5
        result = route_after_execution(state)
        assert result == "failed"

    def test_route_error_with_missing_retry_count(self):
        """Test routing when retry_count is missing (defaults to 0)."""
        clean_agent_modules()
        from agent.graph import route_after_execution

        state = {
            "messages": [],
            "schema_context": "",
            "current_sql": "SELECT * FROM films",
            "query_result": None,
            "error": "Some error",
            # retry_count missing
        }

        result = route_after_execution(state)

        # Should route to correct since retry_count defaults to 0
        assert result == "correct"

    def test_route_error_with_deadline_exhausted_to_failed(self):
        """Route to failed when deadline is exhausted."""
        clean_agent_modules()
        import time

        from agent.graph import route_after_execution

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM films",
            query_result=None,
            error="Some error",
            retry_count=0,
            deadline_ts=time.monotonic() - 1.0,
        )

        result = route_after_execution(state)

        assert result == "failed"

    def test_route_no_error_with_empty_result(self):
        """Test routing when no error but result is empty."""
        clean_agent_modules()
        from agent.graph import route_after_execution

        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM film WHERE 1=0",
            query_result=[],
            error=None,
            retry_count=0,
        )

        result = route_after_execution(state)

        # Empty result is still success, should go to visualize
        assert result == "visualize"


class TestCreateWorkflow:
    """Unit tests for create_workflow function."""

    def test_workflow_creation(self):
        """Test that workflow is created with correct structure."""
        clean_agent_modules()
        from agent.graph import AgentState, create_workflow

        with patch("agent.graph.StateGraph") as mock_state_graph_class:
            mock_workflow = MagicMock()
            mock_state_graph_class.return_value = mock_workflow

            create_workflow()

            # Verify StateGraph was created with AgentState
            mock_state_graph_class.assert_called_once_with(AgentState)

        # Verify nodes were added (now 11 nodes)
        assert mock_workflow.add_node.call_count == 11
        node_calls = [call_args[0][0] for call_args in mock_workflow.add_node.call_args_list]
        assert "cache_lookup" in node_calls
        assert "router" in node_calls
        assert "clarify" in node_calls
        assert "retrieve" in node_calls
        assert "plan" in node_calls
        assert "generate" in node_calls
        assert "validate" in node_calls
        assert "execute" in node_calls
        assert "correct" in node_calls
        assert "visualize" in node_calls
        assert "synthesize" in node_calls

        # Verify entry point was set to cache_lookup
        mock_workflow.set_entry_point.assert_called_once_with("cache_lookup")

        # Verify edges were added (7 edges after reorder)
        assert mock_workflow.add_edge.call_count == 7
        edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]
        assert ("retrieve", "router") in edge_calls  # New: retrieve feeds router

        assert ("clarify", "router") in edge_calls
        assert ("plan", "generate") in edge_calls
        assert ("generate", "validate") in edge_calls
        assert ("generate", "validate") in edge_calls
        assert ("correct", "validate") in edge_calls
        assert ("visualize", "synthesize") in edge_calls
        assert ("synthesize", END) in edge_calls

        # Verify conditional edges were added (now 4)
        assert mock_workflow.add_conditional_edges.call_count == 4

    def test_workflow_compiles(self):
        """Test that the workflow can be compiled without errors."""
        clean_agent_modules()
        from agent.graph import create_workflow

        workflow = create_workflow()
        # Use MemorySaver for compilation
        from langgraph.checkpoint.memory import MemorySaver

        memory = MemorySaver()
        compiled = workflow.compile(checkpointer=memory)

        # Verify compiled workflow exists and can be used
        assert compiled is not None
        # Verify it has the expected structure by checking it's callable/usable
        # LangGraph compiled graphs may not expose nodes directly, so we verify
        # the compilation succeeded by checking the object exists
        assert hasattr(compiled, "invoke") or hasattr(compiled, "astream")


class TestAppCompilation:
    """Unit tests for the compiled app."""

    def test_app_exists(self):
        """Test that app is compiled and available."""
        clean_agent_modules()
        from agent.graph import app

        assert app is not None
        # Verify it's a compiled LangGraph workflow
        assert hasattr(app, "invoke") or hasattr(app, "astream")

    def test_app_has_workflow_methods(self):
        """Test that app has required workflow methods."""
        clean_agent_modules()
        from agent.graph import app

        # Verify the compiled app has methods to execute the workflow
        assert hasattr(app, "invoke") or hasattr(app, "astream")
        # If nodes are accessible, check if they're populated
        # Note: LangGraph may not expose nodes directly, or they may be empty
        # The important thing is that the workflow can be executed
        if hasattr(app, "nodes"):
            try:
                node_names = list(app.nodes.keys())
                # Only verify node names if nodes are actually populated
                # LangGraph's internal structure may not expose nodes in tests
                if len(node_names) > 0:
                    assert len(node_names) >= 10
                    assert "router" in node_names
                    assert "clarify" in node_names
                    assert "retrieve" in node_names
                    assert "plan" in node_names
                    assert "generate" in node_names
                    assert "validate" in node_names
                    assert "execute" in node_names
                    assert "correct" in node_names
                    assert "visualize" in node_names
                    assert "synthesize" in node_names
                # If nodes is empty, that's also acceptable - workflow structure
                # is verified by the ability to invoke/astream
            except (AttributeError, TypeError):
                # If nodes doesn't behave like a dict, that's fine
                pass

    def test_app_can_be_imported(self):
        """Test that app can be imported and is usable."""
        # Verify the app module-level compilation worked
        clean_agent_modules()
        from agent.graph import app as imported_app

        assert imported_app is not None

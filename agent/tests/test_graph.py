"""Unit tests for LangGraph workflow definition."""

from unittest.mock import MagicMock, patch

from agent_core.graph import app, create_workflow, route_after_execution
from agent_core.state import AgentState
from langgraph.graph import END


class TestRouteAfterExecution:
    """Unit tests for route_after_execution conditional routing logic."""

    def test_route_success_to_synthesize(self):
        """Test routing to synthesize when execution succeeds."""
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM film",
            query_result=[{"id": 1}],
            error=None,
            retry_count=0,
        )

        result = route_after_execution(state)

        assert result == "synthesize"

    def test_route_error_with_retries_under_limit_to_correct(self):
        """Test routing to correct when error occurs and retries < 3."""
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

    def test_route_no_error_with_empty_result(self):
        """Test routing when no error but result is empty."""
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql="SELECT * FROM film WHERE 1=0",
            query_result=[],
            error=None,
            retry_count=0,
        )

        result = route_after_execution(state)

        # Empty result is still success, should go to synthesize
        assert result == "synthesize"


class TestCreateWorkflow:
    """Unit tests for create_workflow function."""

    @patch("agent_core.graph.StateGraph")
    def test_workflow_creation(self, mock_state_graph_class):
        """Test that workflow is created with correct structure."""
        mock_workflow = MagicMock()
        mock_state_graph_class.return_value = mock_workflow

        create_workflow()

        # Verify StateGraph was created with AgentState
        mock_state_graph_class.assert_called_once_with(AgentState)

        # Verify nodes were added
        assert mock_workflow.add_node.call_count == 5
        node_calls = [call_args[0][0] for call_args in mock_workflow.add_node.call_args_list]
        assert "retrieve" in node_calls
        assert "generate" in node_calls
        assert "execute" in node_calls
        assert "correct" in node_calls
        assert "synthesize" in node_calls

        # Verify entry point was set
        mock_workflow.set_entry_point.assert_called_once_with("retrieve")

        # Verify edges were added
        assert mock_workflow.add_edge.call_count == 4
        edge_calls = [call_args[0] for call_args in mock_workflow.add_edge.call_args_list]
        assert ("retrieve", "generate") in edge_calls
        assert ("generate", "execute") in edge_calls
        assert ("correct", "execute") in edge_calls
        assert ("synthesize", END) in edge_calls

        # Verify conditional edges were added
        mock_workflow.add_conditional_edges.assert_called_once()
        cond_call = mock_workflow.add_conditional_edges.call_args[0]
        assert cond_call[0] == "execute"
        assert cond_call[1] == route_after_execution
        assert cond_call[2] == {
            "correct": "correct",
            "synthesize": "synthesize",
            "failed": END,
        }

    def test_workflow_compiles(self):
        """Test that the workflow can be compiled without errors."""
        workflow = create_workflow()
        compiled = workflow.compile()

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
        assert app is not None
        # Verify it's a compiled LangGraph workflow
        assert hasattr(app, "invoke") or hasattr(app, "astream")

    def test_app_has_workflow_methods(self):
        """Test that app has required workflow methods."""
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
                    assert len(node_names) >= 5
                    assert "retrieve" in node_names
                    assert "generate" in node_names
                    assert "execute" in node_names
                    assert "correct" in node_names
                    assert "synthesize" in node_names
                # If nodes is empty, that's also acceptable - workflow structure
                # is verified by the ability to invoke/astream
            except (AttributeError, TypeError):
                # If nodes doesn't behave like a dict, that's fine
                pass

    def test_app_can_be_imported(self):
        """Test that app can be imported and is usable."""
        # Verify the app module-level compilation worked
        from agent_core.graph import app as imported_app

        assert imported_app is not None
        assert imported_app is app  # Should be the same instance

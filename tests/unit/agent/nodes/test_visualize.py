from unittest.mock import patch

from agent.nodes.visualize import visualize_query_node
from agent.state import AgentState


class TestVisualizeNode:
    """Tests for the visualization node."""

    def test_visualize_skips_empty_result(self):
        """Node should skip if query_result is None or empty."""
        state = AgentState(query_result=None, viz_spec=None, viz_reason=None)
        # Mock other fields if needed
        # safely ignoring strict TypedDict for test simplicity

        result = visualize_query_node(state)
        assert result["viz_spec"] is None
        assert "No valid query result" in result["viz_reason"]

    def test_visualize_generates_spec(self):
        """Node should populate viz_spec for valid data."""
        data = [{"cat": "A", "val": 10}, {"cat": "B", "val": 20}]
        state = AgentState(query_result=data, viz_spec=None, viz_reason=None)

        with patch("agent.nodes.visualize.build_vega_lite_spec") as mock_build:
            mock_build.return_value = {"mark": "bar"}

            result = visualize_query_node(state)

            assert result["viz_spec"] == {"mark": "bar"}
            assert "Generated bar chart" in result["viz_reason"]
            mock_build.assert_called_once_with(data)

    def test_visualize_handles_unsupported_data(self):
        """Node should return None spec if builder returns None."""
        data = [{"single_col": 1}]
        state = AgentState(query_result=data)

        with patch("agent.nodes.visualize.build_vega_lite_spec") as mock_build:
            mock_build.return_value = None

            result = visualize_query_node(state)

            assert result["viz_spec"] is None
            assert "Data shape not suitable" in result["viz_reason"]

    def test_visualize_catches_exceptions(self):
        """Node should handle exceptions gracefully."""
        state = AgentState(query_result=[{"a": 1}])

        with patch("agent.nodes.visualize.build_vega_lite_spec") as mock_build:
            mock_build.side_effect = Exception("Boom")

            result = visualize_query_node(state)

            assert result["viz_spec"] is None
            assert "Visualization generation failed: Boom" in result["viz_reason"]

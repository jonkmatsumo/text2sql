"""Unit tests for graph_formatter utility."""

from agent_core.utils.graph_formatter import format_graph_to_markdown


class TestFormatGraphToMarkdown:
    """Tests for format_graph_to_markdown function."""

    def test_format_basic_graph(self):
        """Test formatting a graph with 2 tables and 1 relationship."""
        graph_data = {
            "nodes": [
                {
                    "id": "t1",
                    "name": "Table A",
                    "type": "Table",
                    "description": "First table",
                },
                {
                    "id": "t2",
                    "name": "Table B",
                    "type": "Table",
                    "description": "Second table",
                },
                {
                    "id": "c1",
                    "name": "user_id",
                    "type": "Column",
                    "data_type": "integer",
                    "description": "User identifier",
                },
                {
                    "id": "c2",
                    "name": "order_id",
                    "type": "Column",
                    "data_type": "integer",
                    "description": "Order identifier",
                },
            ],
            "relationships": [
                {"source": "t1", "target": "c1", "type": "HAS_COLUMN"},
                {"source": "t2", "target": "c2", "type": "HAS_COLUMN"},
                {"source": "c1", "target": "t2", "type": "FOREIGN_KEY_TO"},
            ],
        }

        result = format_graph_to_markdown(graph_data)

        # Assertions
        assert isinstance(result, str)
        assert "## Table: Table A" in result
        assert "## Table: Table B" in result
        assert "### Connections" in result
        assert "user_id" in result
        assert "Joins to [Table B] via [user_id]" in result

    def test_format_empty_graph(self):
        """Test formatting an empty graph."""
        graph_data = {"nodes": [], "relationships": []}

        result = format_graph_to_markdown(graph_data)

        assert isinstance(result, str)
        assert result == ""

    def test_format_graph_no_relationships(self):
        """Test formatting a graph with tables but no relationships."""
        graph_data = {
            "nodes": [
                {"id": "t1", "name": "Users", "type": "Table"},
            ],
            "relationships": [],
        }

        result = format_graph_to_markdown(graph_data)

        assert isinstance(result, str)
        assert "## Table: Users" in result
        assert "### Connections" not in result

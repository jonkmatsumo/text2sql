"""Unit tests for graph_formatter utility."""

from agent_core.utils.graph_formatter import format_graph_to_markdown


class TestFormatGraphToMarkdown:
    """Tests for format_graph_to_markdown function."""

    def test_format_basic_graph(self):
        """Test formatting a graph with 2 tables and foreign key."""
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
                    "table": "Table A",
                },
                {
                    "id": "c2",
                    "name": "user_id",
                    "type": "Column",
                    "data_type": "integer",
                    "table": "Table B",
                },
            ],
            "relationships": [
                {"source": "t1", "target": "c1", "type": "HAS_COLUMN"},
                {"source": "t2", "target": "c2", "type": "HAS_COLUMN"},
                {"source": "c1", "target": "c2", "type": "FOREIGN_KEY_TO"},
            ],
        }

        result = format_graph_to_markdown(graph_data)

        assert isinstance(result, str)
        assert "## Table: Table A" in result
        assert "## Table: Table B" in result
        assert "### Joins" in result
        assert "user_id" in result

    def test_format_empty_graph(self):
        """Test formatting an empty graph returns a helpful message."""
        graph_data = {"nodes": [], "relationships": []}

        result = format_graph_to_markdown(graph_data)

        assert isinstance(result, str)
        assert result == "No relevant tables found."

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
        assert "### Joins" not in result

    def test_budget_truncation(self):
        """Test that output is truncated when exceeding budget."""
        # Create a graph with many tables
        nodes = []
        for i in range(50):
            nodes.append(
                {
                    "id": f"t{i}",
                    "name": f"Table_{i}",
                    "type": "Table",
                    "description": "A" * 200,  # Long description
                    "score": 0.9 - (i * 0.01),  # Descending scores
                }
            )
            for j in range(20):
                nodes.append(
                    {
                        "id": f"c{i}_{j}",
                        "name": f"column_{j}",
                        "type": "Column",
                        "data_type": "varchar",
                        "table": f"Table_{i}",
                    }
                )

        relationships = []
        for i in range(50):
            for j in range(20):
                relationships.append(
                    {
                        "source": f"t{i}",
                        "target": f"c{i}_{j}",
                        "type": "HAS_COLUMN",
                    }
                )

        graph_data = {"nodes": nodes, "relationships": relationships}

        # Use a small budget
        result = format_graph_to_markdown(graph_data, max_chars=2000)

        assert len(result) <= 2000
        assert "truncated" in result

    def test_column_limit(self):
        """Test that columns are limited per table."""
        nodes = [
            {"id": "t1", "name": "BigTable", "type": "Table"},
        ]
        for i in range(30):
            nodes.append(
                {
                    "id": f"c{i}",
                    "name": f"col_{i}",
                    "type": "Column",
                    "data_type": "int",
                }
            )

        relationships = [
            {"source": "t1", "target": f"c{i}", "type": "HAS_COLUMN"} for i in range(30)
        ]

        graph_data = {"nodes": nodes, "relationships": relationships}
        result = format_graph_to_markdown(graph_data, max_cols_per_table=5)

        # Should show only 5 columns + "more columns" message
        assert "col_0" in result
        assert "col_4" in result
        assert "more columns" in result

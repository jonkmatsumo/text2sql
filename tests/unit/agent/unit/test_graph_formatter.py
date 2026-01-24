"""Unit tests for graph_formatter utility."""

from agent.utils.graph_formatter import format_graph_to_markdown


class TestFormatGraphToMarkdown:
    """Tests for format_graph_to_markdown function."""

    def test_format_basic_graph(self):
        """Test formatting a graph with 2 tables and foreign key."""
        graph_data = {
            "nodes": [
                {
                    "id": "t1",
                    "name": "TableA",
                    "type": "Table",
                    "description": "First table",
                },
                {
                    "id": "t2",
                    "name": "TableB",
                    "type": "Table",
                    "description": "Second table",
                },
                {
                    "id": "c1",
                    "name": "user_id",
                    "type": "Column",
                    "data_type": "integer",
                    "table": "TableA",
                    "is_primary_key": True,
                },
                {
                    "id": "c2",
                    "name": "user_id_fk",
                    "type": "Column",
                    "data_type": "integer",
                    "table": "TableB",
                },
            ],
            "relationships": [
                {"source": "t1", "target": "c1", "type": "HAS_COLUMN"},
                {"source": "t2", "target": "c2", "type": "HAS_COLUMN"},
                {
                    "source": "c2",
                    "target": "c1",
                    "type": "FOREIGN_KEY_TO",
                },  # TableB.user_id_fk -> TableA.user_id
            ],
        }

        result = format_graph_to_markdown(graph_data)

        assert isinstance(result, str)
        # Check for compact table format
        assert "**TableA** (user_id `pk`)" in result
        assert (
            "**TableB** (user_id_fk `fk`)" in result
        )  # Should be marked `fk` because it's involved in join

        # Check for compact join format
        assert "## Joins" in result
        assert "**TableB** JOIN **TableA** ON user_id_fk" in result

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
        assert "**Users** ()" in result
        assert "## Joins" not in result

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
                    "score": 0.9 - (i * 0.01),  # Descending scores
                }
            )
            # Add a few columns to make it non-empty
            nodes.append({"id": f"c{i}_0", "name": "id", "type": "Column", "table": f"Table_{i}"})

        relationships = []
        for i in range(50):
            relationships.append({"source": f"t{i}", "target": f"c{i}_0", "type": "HAS_COLUMN"})

        graph_data = {"nodes": nodes, "relationships": relationships}

        # Max tables cap should apply first (MAX_TABLES=8 default)
        # But we can override args to test truncation char limit if we want,
        # or test that tables beyond 8 are dropped.

        result_default = format_graph_to_markdown(graph_data)
        assert "more tables omitted" in result_default

        # Test character text truncation with strict limit
        result_strict = format_graph_to_markdown(graph_data, max_chars=50, max_tables=50)
        assert "truncated" in result_strict

    def test_column_limit(self):
        """Test that columns are limited per table."""
        nodes = [
            {"id": "t1", "name": "BigTable", "type": "Table"},
        ]
        # Add 30 columns
        for i in range(30):
            nodes.append(
                {
                    "id": f"c{i}",
                    "name": f"col_{i}",
                    "type": "Column",
                    "table": "BigTable",
                }
            )

        relationships = [
            {"source": "t1", "target": f"c{i}", "type": "HAS_COLUMN"} for i in range(30)
        ]

        graph_data = {"nodes": nodes, "relationships": relationships}

        # Limit to 5 columns
        result = format_graph_to_markdown(graph_data, max_cols_per_table=5)

        # Should show 5 columns
        assert "col_0" in result
        assert "col_4" in result
        # Should not show col_5
        assert "col_6" not in result
        # Should have ellipsis
        assert ", ..." in result

    def test_column_prioritization(self):
        """Test that PKs and Text columns are prioritized over others."""
        nodes = [
            {"id": "t1", "name": "PrioritizedTable", "type": "Table"},
            # 1. PK (Priority 0)
            {
                "id": "c1",
                "name": "my_pk",
                "type": "Column",
                "table": "PrioritizedTable",
                "is_primary_key": True,
            },
            # 2. Text (Priority 2)
            {
                "id": "c2",
                "name": "my_text",
                "type": "Column",
                "table": "PrioritizedTable",
                "data_type": "text",
            },
            # 3. Boring int (Priority 3)
            {
                "id": "c3",
                "name": "my_int",
                "type": "Column",
                "table": "PrioritizedTable",
                "data_type": "int",
            },
        ]

        relationships = [
            {"source": "t1", "target": "c1", "type": "HAS_COLUMN"},
            {"source": "t1", "target": "c2", "type": "HAS_COLUMN"},
            {"source": "t1", "target": "c3", "type": "HAS_COLUMN"},
        ]

        graph_data = {"nodes": nodes, "relationships": relationships}

        # With max_cols=3, all show up.
        # Let's test order/flags.
        result = format_graph_to_markdown(graph_data)

        assert "my_pk `pk`" in result

        # Test that limiting columns keep high priority ones
        # Use max_cols=1 -> should keep PK
        result_limited = format_graph_to_markdown(graph_data, max_cols_per_table=1)
        assert "my_pk" in result_limited
        assert "my_text" not in result_limited

"""Tests for TableFirstRetriever."""

import numpy as np
import pytest

from ingestion.table_first_retriever import (
    DEFAULT_MAX_COLUMNS_PER_TABLE,
    TableFirstRetriever,
    format_column_text,
    format_table_text,
)
from ingestion.vector_indexes import HNSWIndex


class TestFormatFunctions:
    """Tests for text formatting functions."""

    def test_format_table_text_basic(self):
        """Format table with name only."""
        result = format_table_text("users")
        assert result == "Table: users"

    def test_format_table_text_with_description(self):
        """Format table with description."""
        result = format_table_text("users", description="User accounts")
        assert result == "Table: users | Description: User accounts"

    def test_format_column_text_basic(self):
        """Format column with table and column name."""
        result = format_column_text("users", "email")
        assert result == "Table: users | Column: email"

    def test_format_column_text_enriched(self):
        """Format column with full enrichment."""
        result = format_column_text(
            "users",
            "email",
            description="User email address",
            data_type="VARCHAR(255)",
        )
        expected = "Table: users | Column: email | " "Desc: User email address | Type: VARCHAR(255)"
        assert result == expected


class TestTableFirstRetriever:
    """Tests for TableFirstRetriever."""

    @pytest.fixture
    def sample_indexes(self):
        """Create sample table and column indexes."""
        # Table index
        table_index = HNSWIndex(dim=3)
        table_vectors = np.array(
            [
                [1.0, 0.0, 0.0],  # users
                [0.8, 0.2, 0.0],  # orders
                [0.0, 1.0, 0.0],  # products
            ]
        )
        table_ids = [1, 2, 3]
        table_metadata = {
            1: {"name": "users", "description": "User accounts"},
            2: {"name": "orders", "description": "Order transactions"},
            3: {"name": "products", "description": "Product catalog"},
        }
        table_index.add_items(table_vectors, table_ids, table_metadata)

        # Column index
        column_index = HNSWIndex(dim=3)
        column_vectors = np.array(
            [
                [0.9, 0.1, 0.0],  # users.email
                [0.85, 0.15, 0.0],  # users.name
                [0.8, 0.2, 0.0],  # users.created_at
                [0.75, 0.25, 0.0],  # users.id
                [0.7, 0.3, 0.0],  # orders.user_id
                [0.65, 0.35, 0.0],  # orders.total
                [0.1, 0.9, 0.0],  # products.name
                [0.05, 0.95, 0.0],  # products.price
            ]
        )
        column_ids = list(range(100, 108))
        column_metadata = {
            100: {"name": "email", "table": "users", "table_id": 1},
            101: {"name": "name", "table": "users", "table_id": 1},
            102: {"name": "created_at", "table": "users", "table_id": 1},
            103: {"name": "id", "table": "users", "table_id": 1},
            104: {"name": "user_id", "table": "orders", "table_id": 2},
            105: {"name": "total", "table": "orders", "table_id": 2},
            106: {"name": "name", "table": "products", "table_id": 3},
            107: {"name": "price", "table": "products", "table_id": 3},
        }
        column_index.add_items(column_vectors, column_ids, column_metadata)

        return table_index, column_index

    def test_basic_retrieval(self, sample_indexes):
        """Verify basic table-first retrieval returns results."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(table_index, column_index, use_rerank=False)

        query = np.array([1.0, 0.0, 0.0])  # Similar to users table
        result = retriever.retrieve(query, table_k=2, column_k=10)

        assert "tables" in result
        assert "columns" in result
        assert len(result["tables"]) == 2

    def test_column_filtering_by_table(self, sample_indexes):
        """Verify columns are filtered to only retrieved tables."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(table_index, column_index, table_k=1, use_rerank=False)

        # Query similar to users - should only get 'users' table
        query = np.array([1.0, 0.0, 0.0])
        result = retriever.retrieve(query, table_k=1, column_k=10)

        # All columns should be from 'users' table
        for col in result["columns"]:
            assert col.metadata["table"] == "users"

    def test_diversity_cap(self, sample_indexes):
        """Verify diversity cap limits columns per table."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(
            table_index,
            column_index,
            table_k=2,
            max_columns_per_table=2,
            use_rerank=False,
        )

        query = np.array([0.9, 0.1, 0.0])
        result = retriever.retrieve(query)

        # Count columns per table
        table_counts = {}
        for col in result["columns"]:
            table = col.metadata["table"]
            table_counts[table] = table_counts.get(table, 0) + 1

        # No table should exceed max_columns_per_table
        for table, count in table_counts.items():
            assert count <= 2, f"Table {table} has {count} columns, expected max 2"

    def test_default_diversity_cap(self, sample_indexes):
        """Verify default max 3 columns per table."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(table_index, column_index, table_k=2, use_rerank=False)

        query = np.array([0.9, 0.1, 0.0])
        result = retriever.retrieve(query, max_columns_per_table=3)

        # Count columns per table
        table_counts = {}
        for col in result["columns"]:
            table = col.metadata["table"]
            table_counts[table] = table_counts.get(table, 0) + 1

        for table, count in table_counts.items():
            assert count <= DEFAULT_MAX_COLUMNS_PER_TABLE

    def test_empty_tables_returns_empty_columns(self):
        """Verify empty table results returns empty columns."""
        empty_table_index = HNSWIndex(dim=2)
        column_index = HNSWIndex(dim=2)
        column_index.add_items(
            np.array([[1.0, 0.0]]),
            [1],
            {1: {"name": "col", "table": "test", "table_id": 1}},
        )

        retriever = TableFirstRetriever(empty_table_index, column_index, use_rerank=False)

        query = np.array([1.0, 0.0])
        result = retriever.retrieve(query)

        assert result["tables"] == []
        assert result["columns"] == []

    def test_retrieve_with_text_query(self, sample_indexes):
        """Verify retrieve_with_text_query uses embed function."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(table_index, column_index, use_rerank=False)

        # Mock embed function
        def mock_embed(text: str) -> list:
            return [1.0, 0.0, 0.0]

        result = retriever.retrieve_with_text_query(
            "Find user emails",
            embed_func=mock_embed,
            table_k=1,
        )

        assert len(result["tables"]) == 1


class TestTableFirstWithRerank:
    """Tests with reranking enabled."""

    @pytest.fixture
    def sample_indexes(self):
        """Create indexes for rerank tests."""
        table_index = HNSWIndex(dim=3)
        table_vectors = np.array(
            [
                [1.0, 0.0, 0.0],
                [0.0, 1.0, 0.0],
            ]
        )
        table_ids = [1, 2]
        table_metadata = {
            1: {"name": "users"},
            2: {"name": "products"},
        }
        table_index.add_items(table_vectors, table_ids, table_metadata)

        column_index = HNSWIndex(dim=3)
        column_vectors = np.array(
            [
                [0.9, 0.1, 0.0],
                [0.8, 0.2, 0.0],
                [0.1, 0.9, 0.0],
            ]
        )
        column_ids = [100, 101, 102]
        column_metadata = {
            100: {"name": "email", "table": "users", "table_id": 1},
            101: {"name": "name", "table": "users", "table_id": 1},
            102: {"name": "price", "table": "products", "table_id": 2},
        }
        column_index.add_items(column_vectors, column_ids, column_metadata)

        return table_index, column_index

    def test_retrieval_with_rerank(self, sample_indexes):
        """Verify retrieval works with reranking enabled."""
        table_index, column_index = sample_indexes
        retriever = TableFirstRetriever(
            table_index, column_index, use_rerank=True, rerank_expansion=5
        )

        query = np.array([1.0, 0.0, 0.0])
        result = retriever.retrieve(query, table_k=1)

        assert len(result["tables"]) == 1
        assert result["tables"][0].metadata["name"] == "users"

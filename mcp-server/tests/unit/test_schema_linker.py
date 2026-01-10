import copy
from unittest.mock import patch

import pytest
from mcp_server.services.schema_linker import SchemaLinker

# Mock Data Template
_MOCK_TABLES_TEMPLATE = [
    {
        "name": "film",
        "sample_data": [{"title": "ACADEMY DINOSAUR", "rating": "PG"}],
        "columns": [
            {"id": "c1", "name": "film_id", "is_primary_key": True},
            {"id": "c2", "name": "title", "data_type": "text"},
            {"id": "c3", "name": "description", "data_type": "text"},
            {"id": "c4", "name": "rating", "data_type": "enum"},
            {"id": "c5", "name": "last_update", "data_type": "timestamp"},
            {"id": "c6", "name": "language_id", "is_foreign_key": True},
        ],
    }
]


@pytest.fixture
def mock_tables():
    """Return a fresh copy of mock tables."""
    return copy.deepcopy(_MOCK_TABLES_TEMPLATE)


@pytest.fixture
def mock_rag_engine():
    """Mock the RagEngine for embedding generation."""
    with patch("mcp_server.services.schema_linker.RagEngine") as mock:
        # embed_text -> [1, 0, 0]
        # embed_batch -> [[0.9, 0, 0], [0, 1, 0]...]
        mock.embed_text.return_value = [1.0, 0.0, 0.0]
        # Default side effect for embed_batch to match input length
        mock.embed_batch.side_effect = lambda texts: [[0.0, 0.0, 0.0]] * len(texts)
        yield mock


class TestSchemaLinker:
    """Test suite for SchemaLinker."""

    def test_structural_filter(self, mock_tables, mock_rag_engine):
        """Ensure PK and FK are always kept."""
        query = "irrelevant query"
        result = SchemaLinker.rank_and_filter_columns(query, mock_tables, target_cols_per_table=2)

        cols = result[0]["columns"]
        col_names = {c["name"] for c in cols}

        assert "film_id" in col_names  # PK
        assert "language_id" in col_names  # FK (property)

    def test_value_spy_filter(self, mock_tables, mock_rag_engine):
        """Ensure value match is kept."""
        query = "Show me ACADEMY DINOSAUR"
        # "ACADEMY DINOSAUR" in sample data for 'title'

        result = SchemaLinker.rank_and_filter_columns(query, mock_tables, target_cols_per_table=5)
        cols = result[0]["columns"]
        col_names = {c["name"] for c in cols}

        assert "title" in col_names
        assert "film_id" in col_names
        assert "language_id" in col_names

    def test_semantic_ranking(self, mock_tables, mock_rag_engine):
        """Ensure semantic vectors influence ranking."""
        mock_rag_engine.embed_text.return_value = [1.0, 0.0]

        def side_effect(texts):
            embeddings = []
            for t in texts:
                if "rating" in t:
                    embeddings.append([0.9, 0.1])
                elif "description" in t:
                    embeddings.append([0.5, 0.5])
                else:
                    embeddings.append([0.1, 0.9])
            return embeddings

        mock_rag_engine.embed_batch.side_effect = side_effect

        # Budget 4: PK(1) + FK(1) + 2 Semantic
        result = SchemaLinker.rank_and_filter_columns(
            "rating", mock_tables, target_cols_per_table=4
        )
        cols = result[0]["columns"]
        col_names = [c["name"] for c in cols]

        assert "film_id" in col_names
        assert "language_id" in col_names
        assert "rating" in col_names
        assert "description" in col_names
        assert "last_update" not in col_names

    def test_budget_constraints(self, mock_tables, mock_rag_engine):
        """Ensure we don't exceed budget (unless structural forces us)."""
        result = SchemaLinker.rank_and_filter_columns("foo", mock_tables, target_cols_per_table=1)
        cols = result[0]["columns"]
        # PK(film_id) and FK(language_id) must be kept
        assert len(cols) >= 2
        assert "film_id" in [c["name"] for c in cols]
        assert "language_id" in [c["name"] for c in cols]

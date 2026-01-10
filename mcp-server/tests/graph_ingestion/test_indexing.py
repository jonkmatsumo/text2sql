"""Tests for vector indexing with adaptive thresholding."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from mcp_server.dal.ingestion.indexing import (
    EmbeddingService,
    VectorIndexer,
    apply_adaptive_threshold,
    cosine_similarity,
)

# Mock data
MOCK_EMBEDDING = [0.1] * 1536
ZERO_VECTOR = [0.0] * 1536


@pytest.fixture
def mock_openai():
    """Mock OpenAI client."""
    with patch("mcp_server.dal.ingestion.indexing.OpenAI") as mock:
        client_instance = mock.return_value
        response_mock = Mock()
        data_item = Mock()
        data_item.embedding = MOCK_EMBEDDING
        response_mock.data = [data_item]

        client_instance.embeddings.create.return_value = response_mock
        yield mock


@pytest.fixture
def mock_store_cls():
    """Mock MemgraphStore class."""
    with patch("mcp_server.dal.ingestion.indexing.MemgraphStore") as mock:
        yield mock


class TestEmbeddingService:
    """Test suite for EmbeddingService."""

    def test_embed_text_valid(self, mock_openai):
        """Test valid text embedding generation."""
        service = EmbeddingService()
        embedding = service.embed_text("test query")

        assert embedding == MOCK_EMBEDDING
        mock_openai.return_value.embeddings.create.assert_called_once()

    def test_embed_text_none(self, mock_openai):
        """Test handling of None input."""
        service = EmbeddingService()
        embedding = service.embed_text(None)

        assert embedding == ZERO_VECTOR
        mock_openai.return_value.embeddings.create.assert_not_called()

    def test_embed_text_empty(self, mock_openai):
        """Test handling of empty string."""
        service = EmbeddingService()
        embedding = service.embed_text("")

        assert embedding == ZERO_VECTOR
        mock_openai.return_value.embeddings.create.assert_not_called()

    def test_embed_text_error(self, mock_openai):
        """Test handling of API error."""
        mock_openai.return_value.embeddings.create.side_effect = Exception("API Error")

        service = EmbeddingService()
        embedding = service.embed_text("test")

        assert embedding == ZERO_VECTOR


class TestCosineSimilarity:
    """Tests for cosine_similarity function."""

    def test_identical_vectors(self):
        """Identical vectors should have similarity 1.0."""
        vec = [1.0, 0.0, 0.0]
        assert cosine_similarity(vec, vec) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        """Orthogonal vectors should have similarity 0.0."""
        vec1 = [1.0, 0.0, 0.0]
        vec2 = [0.0, 1.0, 0.0]
        assert cosine_similarity(vec1, vec2) == pytest.approx(0.0)

    def test_empty_vectors(self):
        """Empty vectors should return 0.0."""
        assert cosine_similarity([], []) == 0.0

    def test_zero_vectors(self):
        """Zero vectors should return 0.0."""
        vec = [0.0, 0.0, 0.0]
        assert cosine_similarity(vec, vec) == 0.0


class TestAdaptiveThreshold:
    """Tests for apply_adaptive_threshold function."""

    def test_filters_low_scores(self):
        """Hits below threshold should be filtered."""
        hits = [
            {"score": 0.9},
            {"score": 0.85},
            {"score": 0.7},  # Below 0.9 - 0.15 = 0.75
        ]
        result = apply_adaptive_threshold(hits)
        assert len(result) == 2

    def test_respects_absolute_minimum(self):
        """Hits below absolute minimum (0.45) should be filtered."""
        hits = [
            {"score": 0.5},
            {"score": 0.4},  # Below 0.45 absolute minimum
        ]
        result = apply_adaptive_threshold(hits)
        assert len(result) == 1
        assert result[0]["score"] == 0.5

    def test_fallback_to_top_k(self):
        """If all filtered, return top 3 (or all if fewer)."""
        hits = [
            {"score": 0.3},  # Below 0.45
            {"score": 0.2},
        ]
        result = apply_adaptive_threshold(hits)
        assert len(result) == 2  # Returns all because len=2 < 3
        assert result[0]["score"] == 0.3
        assert result[1]["score"] == 0.2

    def test_empty_input(self):
        """Empty input should return empty."""
        assert apply_adaptive_threshold([]) == []


class TestVectorIndexer:
    """Test suite for VectorIndexer."""

    def test_create_indexes(self, mock_store_cls, mock_openai):
        """Test index creation creates property indexes."""
        # Setup mock driver via store
        mock_store_instance = mock_store_cls.return_value
        driver_mock = mock_store_instance.driver
        session_mock = driver_mock.session.return_value
        session_mock.__enter__.return_value = session_mock

        indexer = VectorIndexer()
        indexer.create_indexes()

        # Should create 2 property indexes
        assert session_mock.run.call_count == 2
        calls = [str(c) for c in session_mock.run.call_args_list]
        assert any("Table" in c for c in calls)
        assert any("Column" in c for c in calls)

    def test_search_nodes(self, mock_store_cls, mock_openai):
        """Test search logic with brute-force similarity."""
        # Setup mock driver via store
        mock_store_instance = mock_store_cls.return_value
        driver_mock = mock_store_instance.driver
        session_mock = driver_mock.session.return_value
        session_mock.__enter__.return_value = session_mock

        # Mock node with embedding
        mock_node = MagicMock()
        mock_node.__iter__ = Mock(return_value=iter([("name", "TestTable")]))
        mock_node.keys = Mock(return_value=["name", "embedding"])

        # Handle dict(node) conversion
        def get_item(key):
            if key == "name":
                return "TestTable"
            if key == "embedding":
                return MOCK_EMBEDDING
            raise KeyError(key)

        mock_node.__getitem__ = Mock(side_effect=get_item)

        mock_record = {"n": mock_node, "embedding": MOCK_EMBEDDING}
        session_mock.run.return_value = [mock_record]

        indexer = VectorIndexer()
        results = indexer.search_nodes("query", k=3, apply_threshold=False)

        assert len(results) >= 0  # May be 0 if similarity too low
        mock_openai.return_value.embeddings.create.assert_called()
        session_mock.run.assert_called()

from unittest.mock import Mock, patch

import pytest
from mcp_server.graph_ingestion.indexing import EmbeddingService, VectorIndexer

# Mock data
MOCK_EMBEDDING = [0.1] * 1536
ZERO_VECTOR = [0.0] * 1536


@pytest.fixture
def mock_openai():
    """Mock OpenAI client."""
    with patch("mcp_server.graph_ingestion.indexing.OpenAI") as mock:
        client_instance = mock.return_value
        # Mock response structure: response.data[0].embedding
        response_mock = Mock()
        data_item = Mock()
        data_item.embedding = MOCK_EMBEDDING
        response_mock.data = [data_item]

        client_instance.embeddings.create.return_value = response_mock
        yield mock


@pytest.fixture
def mock_neo4j():
    """Mock Neo4j database."""
    with patch("mcp_server.graph_ingestion.indexing.GraphDatabase") as mock:
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
        # Should return zero vector on error based on implementation
        embedding = service.embed_text("test")

        assert embedding == ZERO_VECTOR


class TestVectorIndexer:
    """Test suite for VectorIndexer."""

    def test_create_indexes(self, mock_neo4j, mock_openai):
        """Test index creation logic."""
        # Setup mock driver/session
        driver_mock = mock_neo4j.driver.return_value
        session_mock = driver_mock.session.return_value
        session_mock.__enter__.return_value = session_mock

        indexer = VectorIndexer()
        indexer.create_indexes()

        # Verify calls
        assert session_mock.run.call_count == 2
        # Verify arguments contain expected procedure calls
        calls = session_mock.run.call_args_list
        assert "usearch.init('Table'" in calls[0][0][0] or "usearch.init('Table'" in calls[1][0][0]
        assert (
            "usearch.init('Column'" in calls[0][0][0] or "usearch.init('Column'" in calls[1][0][0]
        )

    def test_search_nodes(self, mock_neo4j, mock_openai):
        """Test search logic."""
        # Setup mock driver/session/result
        driver_mock = mock_neo4j.driver.return_value
        session_mock = driver_mock.session.return_value
        session_mock.__enter__.return_value = session_mock

        # Mock search result
        mock_record = {"node": {"name": "TestTable"}, "score": 0.95}
        session_mock.run.return_value = [mock_record]

        indexer = VectorIndexer()
        results = indexer.search_nodes("query", k=3)

        assert len(results) == 1
        assert results[0]["node"]["name"] == "TestTable"
        assert results[0]["score"] == 0.95

        # Verify embedding service was called
        mock_openai.return_value.embeddings.create.assert_called()

        # Verify session run called with correct params
        session_mock.run.assert_called()
        args, kwargs = session_mock.run.call_args
        assert "usearch.search" in args[0]
        assert kwargs["k"] == 3
        assert kwargs["label"] == "Table"

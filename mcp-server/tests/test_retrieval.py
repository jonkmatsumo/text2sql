import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.graph_ingestion.vector_indexes.protocol import SearchResult
from mcp_server.retrieval import get_relevant_examples


class TestRetrieval:
    """Test suite for retrieval module."""

    @pytest.fixture(autouse=True)
    def reset_index(self):
        """Reset global index before each test."""
        # We need to access the module-level variable to reset it
        # Since it's local to the module, we might need to rely on reloading or a backdoor.
        # But we can patch _get_index or patch the global in a setup.
        # Actually, let's just patch `mcp_server.retrieval._index`
        with patch("mcp_server.retrieval._index", None):
            yield

    @pytest.mark.asyncio
    async def test_get_relevant_examples(self):
        """Test retrieving examples via vector index."""
        mock_index = MagicMock()
        mock_search_result = SearchResult(
            id=1, score=0.9, metadata={"question": "Q1", "sql": "SELECT 1"}
        )
        mock_index.search.return_value = [mock_search_result]

        mock_loader = AsyncMock()

        with patch("mcp_server.retrieval.create_vector_index", return_value=mock_index):
            with patch("mcp_server.retrieval.ExampleLoader", return_value=mock_loader):
                with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=[0.1] * 384):

                    result_json = await get_relevant_examples("query")

                    # Verify loader usage
                    mock_loader.load_examples.assert_called_once_with(mock_index)

                    # Verify search call
                    mock_index.search.assert_called_once()

                    # Verify result format
                    results = json.loads(result_json)
                    assert len(results) == 1
                    assert results[0]["question"] == "Q1"
                    assert results[0]["sql"] == "SELECT 1"
                    assert results[0]["similarity"] == 0.9

    @pytest.mark.asyncio
    async def test_get_relevant_examples_no_results(self):
        """Test when search returns nothing."""
        mock_index = MagicMock()
        mock_index.search.return_value = []
        mock_loader = AsyncMock()

        with patch("mcp_server.retrieval.create_vector_index", return_value=mock_index):
            with patch("mcp_server.retrieval.ExampleLoader", return_value=mock_loader):
                with patch("mcp_server.retrieval.RagEngine.embed_text", return_value=[0.1] * 384):
                    result = await get_relevant_examples("query")
                    assert result == ""

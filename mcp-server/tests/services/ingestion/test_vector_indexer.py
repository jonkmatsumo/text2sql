from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.services.ingestion.vector_indexer import VectorIndexer, apply_adaptive_threshold


class TestVectorIndexerCharacterization:
    """Characterization tests for VectorIndexer Phase 2a."""

    @pytest.fixture
    def mock_store(self):
        """Fixture for MemgraphStore mock."""
        store = MagicMock()
        store.driver.session.return_value.__enter__.return_value = MagicMock()
        return store

    @pytest.fixture
    def indexer(self, mock_store):
        """Fixture for VectorIndexer with patched dependencies."""
        # Patch AsyncOpenAI to prevent initialization error
        # mock_openai is used to suppress the init, valid use even if variable unused
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            indexer = VectorIndexer(store=mock_store)
            # Mock embedding service method to strictly avoid API calls
            indexer.embedding_service.embed_text = AsyncMock(return_value=[0.1] * 1536)
            return indexer

    @pytest.mark.asyncio
    async def test_search_nodes_output_shape(self, indexer):
        """Validate output is list of dicts with node and score."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value

        # Mock node as a plain dict (Memgraph Node behaves like dict)
        mock_node = {"name": "test_table", "embedding": [0.1] * 1536}

        # Record structure matches ANN return: {"node": node, "score": score}
        mock_record = {"node": mock_node, "score": 0.95}
        mock_session.run.return_value = [mock_record]

        # Mocks
        indexer._build_ann_query = MagicMock(return_value="CALL vector_search...")
        indexer._map_ann_results = MagicMock(
            return_value={"node": {"name": "test_table"}, "score": 0.95}
        )

        results = await indexer.search_nodes("query", k=1)

        assert isinstance(results, list)
        assert len(results) == 1
        item = results[0]
        assert item["score"] == 0.95
        assert item["node"]["name"] == "test_table"

    @pytest.mark.asyncio
    async def test_search_nodes_delegates_to_ann_result_order(self, indexer):
        """Validate results are returned in the order provided by the DB (ANN/Scan)."""
        mock_session = indexer.store.driver.session.return_value.__enter__.return_value
        indexer.embedding_service.embed_text = AsyncMock(return_value=[1.0, 0.0])

        # Mock DB returning sorted results
        rec1 = {"node": {"name": "good"}, "score": 0.9}
        rec2 = {"node": {"name": "bad"}, "score": 0.1}

        mock_session.run.return_value = [rec1, rec2]

        # We need _map_ann_results to work for real or be mocked to pass through
        indexer._map_ann_results = lambda r: {"node": r["node"], "score": r["score"]}

        results = await indexer.search_nodes("query", k=2, apply_threshold=False)

        assert len(results) == 2
        assert results[0]["node"]["name"] == "good"
        assert results[0]["score"] > results[1]["score"]


class TestAdaptiveThresholdCharacterization:
    """Characterization tests for apply_adaptive_threshold logic."""

    def test_threshold_filtering(self):
        """Validate basic filtering based on best score."""
        # best_score = 0.9. threshold = max(0.45, 0.9 - 0.15) = 0.75
        hits = [
            {"score": 0.9, "id": 1},
            {"score": 0.8, "id": 2},  # keep (> 0.75)
            {"score": 0.7, "id": 3},  # drop (< 0.75)
        ]
        filtered = apply_adaptive_threshold(hits)
        assert len(filtered) == 2
        ids = [h["id"] for h in filtered]
        assert ids == [1, 2]

    def test_threshold_fallback(self):
        """Validate fallback when all items are filtered out."""
        # best_score = 0.4. threshold = max(0.45, 0.4 - 0.15) = 0.45
        # all items below 0.45
        hits = [
            {"score": 0.40, "id": 1},
            {"score": 0.39, "id": 2},
            {"score": 0.38, "id": 3},
            {"score": 0.37, "id": 4},
        ]
        # Should return top 3 fallback
        filtered = apply_adaptive_threshold(hits)
        assert len(filtered) == 3
        ids = [h["id"] for h in filtered]
        assert ids == [1, 2, 3]

    def test_min_score_absolute(self):
        """Validate MIN_SCORE_ABSOLUTE floor."""
        # best_score = 0.55. threshold = max(0.45, 0.55 - 0.15 = 0.40) = 0.45
        # So threshold is 0.45
        hits = [
            {"score": 0.55, "id": 1},  # keep
            {"score": 0.46, "id": 2},  # keep
            {"score": 0.44, "id": 3},  # drop
        ]
        filtered = apply_adaptive_threshold(hits)
        assert len(filtered) == 2


class TestVectorIndexerANNHelpers:
    """Tests for Phase 2b ANN helpers."""

    @pytest.fixture
    def indexer(self):
        """Fixture for VectorIndexer (mock store not needed for these helpers)."""
        # We only need instances for helper methods, mock store to avoid init errors
        store = MagicMock()
        with patch("mcp_server.services.ingestion.vector_indexer.AsyncOpenAI"):
            return VectorIndexer(store=store)

    def test_build_ann_query_table(self, indexer):
        """Validate query construction for Table label."""
        query = indexer._build_ann_query("Table", "embedding", "$emb", "$k")

        assert "call vector_search.search" in query.lower()
        assert "'table_embedding_index'" in query
        assert "'Table'" in query
        assert "'embedding'" in query
        assert "$emb" in query
        assert "$k" in query
        assert "YIELD node, score" in query
        assert "RETURN node, score" in query

    def test_build_ann_query_column(self, indexer):
        """Validate query construction for Column label uses fallback Cypher scan."""
        query = indexer._build_ann_query("Column", "embedding", "$emb", "$k")

        query_lower = query.lower()
        assert "call vector_search.search" not in query_lower
        assert "match (node:column)" in query_lower
        assert "vector.similarity.cosine" in query_lower
        assert "limit $k" in query_lower

    def test_map_ann_results_success(self, indexer):
        """Validate mapping of correct record."""
        mock_node = {"name": "test", "embedding": [0.1]}
        record = {"node": mock_node, "score": 0.95}

        result = indexer._map_ann_results(record)

        assert result["score"] == 0.95
        assert result["node"]["name"] == "test"
        assert "embedding" not in result["node"]

    def test_map_ann_results_score_type_handling(self, indexer):
        """Validate robustness to score types."""
        mock_node = {"name": "test"}
        # integer score
        record = {"node": mock_node, "score": 1}
        result = indexer._map_ann_results(record)
        assert isinstance(result["score"], float)
        assert result["score"] == 1.0

        # string score (shouldn't happen but good defense)
        record = {"node": mock_node, "score": "0.5"}
        result = indexer._map_ann_results(record)
        assert result["score"] == 0.5

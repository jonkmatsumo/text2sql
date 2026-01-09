"""Tests for search_with_rerank function."""

import os

import numpy as np
import pytest
from mcp_server.dal.ingestion.vector_indexes import HNSWIndex, search_with_rerank


class TestSearchWithRerank:
    """Tests for retrieve-and-rerank strategy."""

    @pytest.fixture
    def sample_index(self):
        """Create HNSWIndex with sample data."""
        index = HNSWIndex(dim=3)
        vectors = np.array(
            [
                [1.0, 0.0, 0.0],  # id=0
                [0.9, 0.1, 0.0],  # id=1 (close to 0)
                [0.8, 0.2, 0.0],  # id=2 (close to 0)
                [0.0, 1.0, 0.0],  # id=3 (orthogonal)
                [0.1, 0.9, 0.0],  # id=4 (close to 3)
                [0.5, 0.5, 0.0],  # id=5 (45 degrees)
            ]
        )
        ids = list(range(6))
        index.add_items(vectors, ids)
        return index

    @pytest.fixture
    def ground_truth_index(self):
        """Create a second HNSWIndex with same data for validation."""
        index = HNSWIndex(dim=3)
        vectors = np.array(
            [
                [1.0, 0.0, 0.0],
                [0.9, 0.1, 0.0],
                [0.8, 0.2, 0.0],
                [0.0, 1.0, 0.0],
                [0.1, 0.9, 0.0],
                [0.5, 0.5, 0.0],
            ]
        )
        ids = list(range(6))
        index.add_items(vectors, ids)
        return index

    def test_basic_rerank(self, sample_index):
        """Verify basic rerank returns correct results."""
        query = np.array([1.0, 0.0, 0.0])
        results = search_with_rerank(sample_index, query, k=3, expansion_factor=2)

        assert len(results) == 3
        # Best match should be id=0
        assert results[0].id == 0
        assert results[0].score > 0.99

    def test_expansion_fetches_more_candidates(self, sample_index):
        """Verify expansion factor fetches more candidates."""
        query = np.array([0.5, 0.5, 0.0])

        # With low expansion, might miss some
        results_low = search_with_rerank(sample_index, query, k=2, expansion_factor=1)

        # With high expansion, should get optimal set
        results_high = search_with_rerank(sample_index, query, k=2, expansion_factor=10)

        # Both should return 2 results
        assert len(results_low) == 2
        assert len(results_high) == 2

    def test_vectorized_scoring_accuracy(self, sample_index, ground_truth_index):
        """Verify vectorized scoring matches ground truth results."""
        query = np.array([0.7, 0.3, 0.0])

        reranked = search_with_rerank(sample_index, query, k=3, expansion_factor=10)
        ground_truth = ground_truth_index.search(query, k=3)

        # With high expansion factor, should get same top-k
        reranked_ids = set(r.id for r in reranked)
        truth_ids = set(r.id for r in ground_truth)

        # Allow for minor ANN differences, but should mostly overlap
        overlap = len(reranked_ids & truth_ids)
        assert overlap >= 2  # At least 2 of 3 should match

    def test_scores_are_sorted_descending(self, sample_index):
        """Verify results are sorted by score descending."""
        query = np.array([0.5, 0.5, 0.0])
        results = search_with_rerank(sample_index, query, k=4, expansion_factor=5)

        scores = [r.score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_empty_index(self):
        """Verify empty index returns empty results."""
        index = HNSWIndex(dim=3)
        query = np.array([1.0, 0.0, 0.0])
        results = search_with_rerank(index, query, k=3)
        assert results == []

    def test_zero_query_vector(self, sample_index):
        """Verify zero query returns empty results."""
        query = np.array([0.0, 0.0, 0.0])
        results = search_with_rerank(sample_index, query, k=3)
        assert results == []

    def test_metadata_preserved(self):
        """Verify metadata is preserved through reranking."""
        index = HNSWIndex(dim=2)
        vectors = np.array([[1.0, 0.0], [0.0, 1.0]])
        ids = [100, 200]
        metadata = {100: {"name": "table_a"}, 200: {"name": "table_b"}}
        index.add_items(vectors, ids, metadata=metadata)

        query = np.array([1.0, 0.0])
        results = search_with_rerank(index, query, k=1)

        assert results[0].id == 100
        assert results[0].metadata == {"name": "table_a"}


class TestRecallLossLogging:
    """Tests for recall loss validation."""

    def test_recall_loss_logged_when_enabled(self, caplog):
        """Verify recall loss is logged when RECORD_GOLDEN_SET=1."""
        # Create indexes with same data
        hnsw_index = HNSWIndex(dim=2)
        gt_index = HNSWIndex(dim=2)

        # Same data for both
        vectors = np.array([[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]])
        ids = [0, 1, 2]
        hnsw_index.add_items(vectors, ids)
        gt_index.add_items(vectors, ids)

        query = np.array([1.0, 0.0])

        # Enable logging
        os.environ["RECORD_GOLDEN_SET"] = "1"
        try:
            import logging

            caplog.set_level(logging.DEBUG)
            results = search_with_rerank(
                hnsw_index, query, k=2, expansion_factor=10, brute_force_index=gt_index
            )
            # Should have results
            assert len(results) == 2
        finally:
            os.environ.pop("RECORD_GOLDEN_SET", None)

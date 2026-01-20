from unittest.mock import MagicMock, patch

import pytest

from dal.memgraph.graph_store import MemgraphStore


class MockNode(dict):
    """Mock Neo4j node that behaves like a dict but has attributes."""

    def __init__(self, data, element_id, labels):
        """Initialize mock node."""
        super().__init__(data)
        self.element_id = element_id
        self.labels = labels


class TestMemgraphStoreANN:
    """Tests for MemgraphStore ANN search methods."""

    @pytest.fixture
    def store(self):
        """Create a MemgraphStore instance with mocked driver."""
        with patch("dal.memgraph.graph_store.GraphDatabase.driver"):
            store = MemgraphStore("bolt://localhost:7687", "user", "pass")
            return store

    def test_search_ann_seeds_client_side_cosine(self, store):
        """Verify client-side cosine similarity fallback."""
        mock_session = MagicMock()
        store.driver.session.return_value.__enter__.return_value = mock_session

        # Setup 2 nodes with embeddings
        # Query vector: [1.0, 0.0]
        # Node 1: [1.0, 0.0] -> dot=1.0, mag=1.0 -> score 1.0
        # Node 2: [0.0, 1.0] -> dot=0.0, mag=1.0 -> score 0.0
        mock_node_1 = MockNode(
            {"id": "1", "name": "exact_match", "embedding": [1.0, 0.0]}, "1", ["Table"]
        )
        mock_node_2 = MockNode(
            {"id": "2", "name": "no_match", "embedding": [0.0, 1.0]}, "2", ["Table"]
        )

        # Mock results (just "node" key, no DB-side score)
        mock_record_1 = MagicMock()
        mock_record_1.__getitem__.side_effect = lambda k: {"node": mock_node_1}[k]

        mock_record_2 = MagicMock()
        mock_record_2.__getitem__.side_effect = lambda k: {"node": mock_node_2}[k]

        mock_session.run.return_value = [mock_record_1, mock_record_2]

        embedding = [1.0, 0.0]
        hits = store.search_ann_seeds("Table", embedding, k=5)

        assert len(hits) == 2
        # Should be sorted by score DESC
        assert hits[0]["node"]["name"] == "exact_match"
        assert abs(hits[0]["score"] - 1.0) < 1e-6

        assert hits[1]["node"]["name"] == "no_match"
        assert abs(hits[1]["score"] - 0.0) < 1e-6

        # Verify Query structure
        args, kwargs = mock_session.run.call_args
        query = args[0]

        # Check that we are fetching nodes with embeddings, not running vector proc
        assert "MATCH (n:`Table`)" in query
        assert "WHERE n.embedding IS NOT NULL" in query
        assert "RETURN n AS node" in query
        assert "vector.similarity" not in query

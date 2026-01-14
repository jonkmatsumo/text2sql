from unittest.mock import MagicMock, patch

import pytest
from mcp_server.dal.memgraph.graph_store import MemgraphStore


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
        with patch("mcp_server.dal.memgraph.graph_store.GraphDatabase.driver"):
            store = MemgraphStore("bolt://localhost:7687", "user", "pass")
            return store

    def test_search_ann_seeds_table_strategy(self, store):
        """Verify Table strategy uses vector_search module."""
        mock_session = MagicMock()
        store.driver.session.return_value.__enter__.return_value = mock_session

        mock_node = MockNode({"id": "1", "name": "foo"}, "1", ["Table"])

        mock_record = MagicMock()
        mock_record.__getitem__.side_effect = lambda k: {
            "node": mock_node,
            "score": 0.95,
        }[k]

        mock_session.run.return_value = [mock_record]

        embedding = [0.1, 0.2]
        hits = store.search_ann_seeds("Table", embedding, k=5)

        assert len(hits) == 1
        # Check flat node properties
        assert hits[0]["node"]["name"] == "foo"
        assert hits[0]["score"] == 0.95

        # Verify Query
        args, kwargs = mock_session.run.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else kwargs

        assert "CALL vector_search.search($index, $label, $prop, $vector, $k)" in query
        assert params["index"] == "table_embedding_index"
        assert params["label"] == "Table"
        assert params["vector"] == embedding
        assert params["k"] == 5

    def test_search_ann_seeds_fallback_strategy(self, store):
        """Verify Fallback strategy uses cosine scan."""
        mock_session = MagicMock()
        store.driver.session.return_value.__enter__.return_value = mock_session

        mock_node = MockNode({"id": "2", "col": "c"}, "2", ["Column"])

        mock_record = MagicMock()
        mock_record.__getitem__.side_effect = lambda k: {
            "node": mock_node,
            "score": 0.8,
        }[k]

        mock_session.run.return_value = [mock_record]

        embedding = [0.1, 0.2]
        hits = store.search_ann_seeds("Column", embedding, k=3)

        assert len(hits) == 1
        assert hits[0]["node"]["col"] == "c"

        # Verify Query
        args, _ = mock_session.run.call_args
        query = args[0]

        assert "vector.similarity.cosine" in query
        assert "MATCH (n:`Column`)" in query
        assert "LIMIT $k" in query

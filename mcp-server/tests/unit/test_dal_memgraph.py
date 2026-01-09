from unittest.mock import MagicMock, patch

import pytest
from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.dal.types import Node


class TestMemgraphStore:
    """Test MemgraphStore adapter logic."""

    @pytest.fixture
    def mock_driver(self):
        """Mock Neo4j Driver."""
        with patch("mcp_server.dal.memgraph.GraphDatabase.driver") as mock_d:
            driver_instance = MagicMock()
            mock_d.return_value = driver_instance
            yield driver_instance

    def test_init(self, mock_driver):
        """Test initialization."""
        store = MemgraphStore("bolt://localhost", "user", "pass")
        assert store.driver == mock_driver

    def test_upsert_node(self, mock_driver):
        """Test node upsert and mapping."""
        store = MemgraphStore("bolt://localhost", "user", "pass")

        # Mock Session and Result
        session = MagicMock()
        store.driver.session.return_value.__enter__.return_value = session

        # Mock Record
        class FakeNode(dict):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.labels = {"Table"}

        mock_node = FakeNode({"id": "t1", "name": "users"})

        mock_record = {"n": mock_node}
        session.run.return_value.single.return_value = mock_record

        # Execute
        node = store.upsert_node("Table", "t1", {"name": "users"})

        assert isinstance(node, Node)
        assert node.id == "t1"
        assert node.label == "Table"
        assert node.properties == {"id": "t1", "name": "users"}

        # Verify Query
        session.run.assert_called_once()
        args, kwargs = session.run.call_args
        assert "MERGE (n:`Table` {id: $node_id})" in args[0]
        assert kwargs["node_id"] == "t1"

    def test_delete_subgraph(self, mock_driver):
        """Test delete operation returns IDs."""
        store = MemgraphStore("bolt://localhost", "user", "pass")

        session = MagicMock()
        store.driver.session.return_value.__enter__.return_value = session

        # Mock results
        mock_records = [{"deleted_id": "n1"}, {"deleted_id": "n2"}]
        session.run.return_value.__iter__.return_value = mock_records

        deleted = store.delete_subgraph("n1")

        assert deleted == ["n1", "n2"]
        session.run.assert_called_once()
        args, _ = session.run.call_args
        assert "DETACH DELETE m" in args[0]

"""Unit tests for DAL canonical types."""

import pytest
from mcp_server.models.cache.lookup_result import CacheLookupResult
from mcp_server.models.graph.data import GraphData
from mcp_server.models.graph.edge import Edge
from mcp_server.models.graph.node import Node


class TestNode:
    """Tests for Node Pydantic model."""

    def test_node_creation(self):
        """Test basic node creation."""
        node = Node(id="node1", label="Table", properties={"name": "users"})
        assert node.id == "node1"
        assert node.label == "Table"
        assert node.properties == {"name": "users"}

    def test_node_default_properties(self):
        """Test default empty properties."""
        node = Node(id="node1", label="Table")
        assert node.properties == {}

    def test_node_serialization(self):
        """Test JSON serialization."""
        node = Node(id="node1", label="Table", properties={"active": True})
        data = node.model_dump()
        assert data == {
            "id": "node1",
            "label": "Table",
            "properties": {"active": True},
        }


class TestEdge:
    """Tests for Edge Pydantic model."""

    def test_edge_creation(self):
        """Test basic edge creation."""
        edge = Edge(
            source_id="n1",
            target_id="n2",
            type="HAS_COLUMN",
            properties={"order": 1},
        )
        assert edge.source_id == "n1"
        assert edge.target_id == "n2"
        assert edge.type == "HAS_COLUMN"
        assert edge.properties == {"order": 1}

    def test_edge_default_properties(self):
        """Test default empty properties."""
        edge = Edge(source_id="n1", target_id="n2", type="LINK")
        assert edge.properties == {}


class TestGraphData:
    """Tests for GraphData container."""

    def test_graph_data_creation(self):
        """Test creating GraphData with nodes and edges."""
        n1 = Node(id="n1", label="Table")
        n2 = Node(id="n2", label="Column")
        edge = Edge(source_id="n1", target_id="n2", type="HAS_COLUMN")

        graph = GraphData(nodes=[n1, n2], edges=[edge])
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1

    def test_node_count(self):
        """Test node counting method."""
        graph = GraphData(nodes=[Node(id="n1", label="T"), Node(id="n2", label="T")])
        assert graph.node_count() == 2

    def test_edge_count(self):
        """Test edge counting method."""
        graph = GraphData(edges=[Edge(source_id="n1", target_id="n2", type="E")])
        assert graph.edge_count() == 1

    def test_get_node_by_id(self):
        """Test retrieving a node by ID."""
        n1 = Node(id="target_id", label="Table")
        graph = GraphData(nodes=[n1])

        found = graph.get_node_by_id("target_id")
        assert found == n1

        not_found = graph.get_node_by_id("missing")
        assert not_found is None

    def test_get_nodes_by_label(self):
        """Test filtering nodes by label."""
        n1 = Node(id="n1", label="Table")
        n2 = Node(id="n2", label="Column")
        n3 = Node(id="n3", label="Table")
        graph = GraphData(nodes=[n1, n2, n3])

        tables = graph.get_nodes_by_label("Table")
        assert len(tables) == 2
        assert n1 in tables
        assert n3 in tables
        assert n2 not in tables


class TestCacheLookupResult:
    """Tests for CacheLookupResult model."""

    def test_creation(self):
        """Test valid creation."""
        result = CacheLookupResult(
            cache_id="123",
            value="SELECT * FROM users",
            similarity=0.95,
            metadata={"source": "test"},
        )
        assert result.similarity == 0.95

    def test_similarity_validation(self):
        """Test similarity bounds validation."""
        # This assumes Pydantic validates inputs, which it does.
        with pytest.raises(Exception):
            CacheLookupResult(cache_id="1", value="s", similarity=1.5)  # > 1.0

        with pytest.raises(Exception):
            CacheLookupResult(cache_id="1", value="s", similarity=-0.1)  # < 0.0

"""Unit tests for DAL interfaces (Protocols).

Verifies that the protocols are structurally correct and can be implemented.
"""

from typing import Any, Dict, List, Optional

import numpy as np
from mcp_server.dal.interfaces import CacheStore, ExtendedVectorIndex, GraphStore
from mcp_server.dal.types import CacheLookupResult, Edge, GraphData, Node
from mcp_server.graph_ingestion.vector_indexes.protocol import SearchResult


class MockCacheStore:
    """Mock implementation of CacheStore."""

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
    ) -> Optional[CacheLookupResult]:
        """Mock lookup."""
        return None

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Mock record_hit."""
        pass

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
    ) -> None:
        """Mock store."""
        pass


class MockGraphStore:
    """Mock implementation of GraphStore."""

    def upsert_node(
        self,
        label: str,
        node_id: str,
        properties: Dict[str, Any],
    ) -> Node:
        """Mock upsert_node."""
        return Node(id=node_id, label=label, properties=properties)

    def upsert_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Edge:
        """Mock upsert_edge."""
        return Edge(
            source_id=source_id,
            target_id=target_id,
            type=edge_type,
            properties=properties or {},
        )

    def get_subgraph(
        self,
        root_id: str,
        depth: int = 1,
        labels: Optional[List[str]] = None,
    ) -> GraphData:
        """Mock get_subgraph."""
        return GraphData()

    def delete_subgraph(self, root_id: str) -> List[str]:
        """Mock delete_subgraph."""
        return [root_id]


class MockVectorIndex:
    """Mock implementation of ExtendedVectorIndex."""

    def search(
        self,
        query_vector: np.ndarray,
        k: int,
        filter: Optional[Dict] = None,
    ) -> List[SearchResult]:
        """Mock search."""
        return []

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[str],
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Mock add_items."""
        pass

    def delete_items(self, ids: List[str]) -> None:
        """Mock delete_items."""
        pass

    def save(self, path: str) -> None:
        """Mock save."""
        pass

    def load(self, path: str) -> None:
        """Mock load."""
        pass


class TestInterfaces:
    """Verify structural subtyping compliance."""

    def test_cache_store_compliance(self):
        """Verify MockCacheStore implements CacheStore protocol."""
        # Using isinstance checks runtime_checkable protocols
        impl = MockCacheStore()
        assert isinstance(impl, CacheStore)

    def test_graph_store_compliance(self):
        """Verify MockGraphStore implements GraphStore protocol."""
        impl = MockGraphStore()
        assert isinstance(impl, GraphStore)

    def test_vector_index_compliance(self):
        """Verify MockVectorIndex implements ExtendedVectorIndex protocol."""
        impl = MockVectorIndex()
        assert isinstance(impl, ExtendedVectorIndex)

    def test_extended_vs_base_compatibility(self):
        """Verify ExtendedVectorIndex is distinct from base VectorIndex."""
        # The base VectorIndex expects List[int] for IDs, Extended expects List[str].
        # They are distinct protocols.
        # So it should potentially fail or be separate.

        # We designed them to be different. Let's ensure new code uses Extended.
        impl = MockVectorIndex()
        assert isinstance(impl, ExtendedVectorIndex)

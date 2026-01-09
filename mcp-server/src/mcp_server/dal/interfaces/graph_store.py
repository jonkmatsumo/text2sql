from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from mcp_server.models.graph.data import GraphData
from mcp_server.models.graph.edge import Edge
from mcp_server.models.graph.node import Node


@runtime_checkable
class GraphStore(Protocol):
    """Protocol for graph database backends.

    Implementations must provide CRUD operations and return canonical
    Node/Edge/GraphData types (not raw driver objects).

    The delete_subgraph method returns deleted node IDs to allow
    synchronization with other stores (e.g., removing vectors from VectorIndex).
    """

    def upsert_node(
        self,
        label: str,
        node_id: str,
        properties: Dict[str, Any],
    ) -> Node:
        """Create or update a node.

        Args:
            label: Node type/label (e.g., "Table", "Column").
            node_id: Unique identifier for the node.
            properties: Node properties to set.

        Returns:
            The canonical Node representation.
        """
        ...

    def upsert_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Edge:
        """Create or update an edge between two nodes.

        Args:
            source_id: ID of the source node.
            target_id: ID of the target node.
            edge_type: Relationship type (e.g., "HAS_COLUMN").
            properties: Optional edge properties.

        Returns:
            The canonical Edge representation.
        """
        ...

    def get_subgraph(
        self,
        root_id: str,
        depth: int = 1,
        labels: Optional[List[str]] = None,
    ) -> GraphData:
        """Retrieve a subgraph starting from a root node.

        CRITICAL: Must return canonical GraphData, not raw driver objects.

        Args:
            root_id: ID of the starting node.
            depth: How many hops to traverse (default 1).
            labels: Optional list of labels to filter traversal.

        Returns:
            GraphData containing all nodes and edges in the subgraph.
        """
        ...

    def delete_subgraph(self, root_id: str) -> List[str]:
        """Delete a subgraph starting from a root node.

        Returns the IDs of all deleted nodes to allow synchronization
        with other stores (e.g., removing vectors from VectorIndex).

        Args:
            root_id: ID of the root node to delete.

        Returns:
            List of deleted node IDs.
        """
        ...

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from mcp_server.models import Edge, GraphData, Node


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

    def get_nodes(self, label: str) -> List[Node]:
        """Retrieve all nodes with a specific label.

        Args:
            label: Node label (e.g., "Table").

        Returns:
            List of canonical Node representations.
        """
        ...

    def run_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Run a raw Cypher/SQL query and return results as dictionaries.

        Use ONLY for complex operations not covered by standard CRUD methods.
        Implementations must handle parameter binding to prevent injection.

        Args:
            query: The raw query string.
            parameters: Optional query parameters.

        Returns:
            List of result records as dictionaries.
        """
        ...

    def search_ann_seeds(
        self,
        label: str,
        embedding: List[float],
        k: int,
        index_name: str = "table_embedding_index",
        embedding_property: str = "embedding",
    ) -> List[Dict[str, Any]]:
        """Search for seeds using vector similarity.

        Args:
            label: Node label to search (e.g., "Table").
            embedding: The query vector.
            k: Number of hits to return.
            index_name: Name of the vector index to use (if applicable).
            embedding_property: Property containing the vector.

        Returns:
            List of dicts: {"node": dict, "score": float}
        """
        ...

    def close(self):
        """Close the store connection."""
        ...

from typing import List, Optional

from pydantic import BaseModel, Field

from .edge import Edge
from .node import Node


class GraphData(BaseModel):
    """Container for graph query results.

    This is the standard return type for all graph operations that
    return subgraphs, traversals, or multi-node results.

    Attributes:
        nodes: List of nodes in the result.
        edges: List of edges in the result.
    """

    nodes: List[Node] = Field(default_factory=list)
    edges: List[Edge] = Field(default_factory=list)

    def node_count(self) -> int:
        """Return the number of nodes."""
        return len(self.nodes)

    def edge_count(self) -> int:
        """Return the number of edges."""
        return len(self.edges)

    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """Find a node by its ID.

        Args:
            node_id: The ID of the node to find.

        Returns:
            The node if found, None otherwise.
        """
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def get_nodes_by_label(self, label: str) -> List[Node]:
        """Get all nodes with a specific label.

        Args:
            label: The label to filter by.

        Returns:
            List of nodes with the specified label.
        """
        return [node for node in self.nodes if node.label == label]

    model_config = {"frozen": False}

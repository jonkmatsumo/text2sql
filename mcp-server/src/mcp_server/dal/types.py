"""Canonical data types for the Data Abstraction Layer.

These types define the intermediate representation for all DAL operations,
preventing backend-specific objects (Neo4j Node, asyncpg Record, etc.)
from leaking into business logic.
"""

from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field


class Node(BaseModel):
    """Canonical graph node representation.

    All graph store implementations must convert their native node types
    to this canonical representation before returning to business logic.

    Attributes:
        id: Unique identifier (string for cross-backend compatibility).
        label: Node type/label (e.g., "Table", "Column").
        properties: Additional node properties.
    """

    id: str
    label: str
    properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}


class Edge(BaseModel):
    """Canonical graph edge representation.

    All graph store implementations must convert their native edge types
    to this canonical representation before returning to business logic.

    Attributes:
        source_id: ID of the source node.
        target_id: ID of the target node.
        type: Relationship type (e.g., "HAS_COLUMN", "FOREIGN_KEY_TO").
        properties: Additional edge properties.
    """

    source_id: str
    target_id: str
    type: str
    properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}


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

    def get_node_by_id(self, node_id: str) -> Node | None:
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


class CacheLookupResult(BaseModel):
    """Result from a cache lookup operation.

    Returned by CacheStore.lookup() when a cache hit is found.

    Attributes:
        cache_id: Internal database key for the cache entry.
        value: The cached value (e.g., generated SQL).
        similarity: Similarity score (0.0 to 1.0).
        metadata: Additional metadata about the cache entry.
    """

    cache_id: str
    value: str
    similarity: float = Field(ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}


# Type alias for structured filtering.
# Supports operators like {"category": {"$eq": "billing"}}
# Adapters will transpile this to backend-specific syntax (SQL WHERE, Pinecone filter, etc.)
#
# Examples:
#   - Simple equality: {"category": "billing"}
#   - With operator: {"price": {"$gt": 100}}
#   - Multiple conditions: {"category": "billing", "status": {"$in": ["active", "pending"]}}
FilterCriteria = Dict[str, Union[str, int, float, bool, Dict[str, Any]]]


class Example(BaseModel):
    """Canonical representation of a few-shot learning example.

    Attributes:
        id: Unique identifier.
        question: The natural language question.
        sql_query: The corresponding SQL query.
        embedding: The embedding vector of the question.
    """

    id: int
    question: str
    sql_query: str
    embedding: List[float]

    model_config = {"frozen": False}


class SchemaEmbedding(BaseModel):
    """Canonical representation of a table schema embedding.

    Attributes:
        table_name: Name of the table.
        schema_text: Text description of the schema (columns, FKs).
        embedding: The embedding vector of the schema text.
    """

    table_name: str
    schema_text: str
    embedding: List[float]

    model_config = {"frozen": False}


class ColumnDef(BaseModel):
    """Canonical representation of a database column definition."""

    name: str
    data_type: str
    is_nullable: bool

    model_config = {"frozen": False}


class ForeignKeyDef(BaseModel):
    """Canonical representation of a foreign key constraint."""

    column_name: str
    foreign_table_name: str
    foreign_column_name: str

    model_config = {"frozen": False}


class TableDef(BaseModel):
    """Canonical representation of a database table definition."""

    name: str
    columns: List[ColumnDef] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyDef] = Field(default_factory=list)

    model_config = {"frozen": False}

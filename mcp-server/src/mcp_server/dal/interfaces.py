"""Protocol definitions for Data Abstraction Layer.

These protocols define the contracts that all backend adapters must implement.
Using Protocol (structural subtyping) allows duck typing while maintaining
type safety.
"""

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import numpy as np
from mcp_server.dal.ingestion.vector_indexes.protocol import SearchResult
from mcp_server.models.cache.lookup_result import CacheLookupResult
from mcp_server.models.database.table_def import TableDef
from mcp_server.models.graph.data import GraphData
from mcp_server.models.graph.edge import Edge
from mcp_server.models.graph.node import Node
from mcp_server.models.rag.embedding import SchemaEmbedding
from mcp_server.models.rag.example import Example
from mcp_server.models.rag.filters import FilterCriteria


@runtime_checkable
class SchemaIntrospector(Protocol):
    """Protocol for introspecting database schema (tables, columns, constraints)."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the specified schema."""
        ...

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        ...


@runtime_checkable
class SchemaStore(Protocol):
    """Protocol for accessing table schema embeddings.

    Abstracts the storage of schema embeddings (Postgres, localized file, etc.).
    """

    async def fetch_schema_embeddings(self) -> List[SchemaEmbedding]:
        """Fetch all schema embeddings.

        Returns:
            List of canonical SchemaEmbedding objects.
        """
        ...

    async def save_schema_embedding(self, embedding: SchemaEmbedding) -> None:
        """Save (upsert) a schema embedding.

        Args:
            embedding: The schema embedding to save.
        """
        ...


@runtime_checkable
class ExampleStore(Protocol):
    """Protocol for accessing few-shot learning examples.

    This abstracts the source of examples (Postgres, CSV, API, etc.)
    from the retrieval logic.
    """

    async def fetch_all_examples(self) -> List[Example]:
        """Fetch all available examples.

        Returns:
            List of canonical Example objects.
        """
        ...


@runtime_checkable
class CacheStore(Protocol):
    """Protocol for semantic cache backends.

    Implementations must provide:
    - lookup: Pure retrieval without side effects
    - record_hit: Fire-and-forget hit counting (eventual consistency OK)
    - store: Store a new cache entry

    The split between lookup and record_hit accommodates eventual consistency
    stores like Pinecone that lack atomic increment operations.
    """

    async def lookup(
        self,
        query_embedding: List[float],
        tenant_id: int,
        threshold: float = 0.95,
    ) -> Optional[CacheLookupResult]:
        """Lookup a cached result by embedding similarity.

        This is a pure retrieval operation with no side effects.
        Use record_hit() separately to update access statistics.

        Args:
            query_embedding: The embedding vector of the query.
            tenant_id: Tenant identifier for isolation.
            threshold: Minimum similarity threshold (0.0 to 1.0).

        Returns:
            CacheLookupResult if a match is found above threshold, None otherwise.
        """
        ...

    async def record_hit(self, cache_id: str, tenant_id: int) -> None:
        """Record a cache hit for statistics.

        This is a fire-and-forget operation. Implementations may use
        eventual consistency (e.g., async queue to Redis sidecar).

        Args:
            cache_id: The cache entry identifier.
            tenant_id: Tenant identifier for isolation.
        """
        ...

    async def store(
        self,
        user_query: str,
        generated_sql: str,
        query_embedding: List[float],
        tenant_id: int,
    ) -> None:
        """Store a new cache entry.

        Args:
            user_query: The original user query text.
            generated_sql: The generated SQL to cache.
            query_embedding: The embedding vector of the query.
            tenant_id: Tenant identifier for isolation.
        """
        ...


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


@runtime_checkable
class MetadataStore(Protocol):
    """Protocol for high-level database metadata access (used by Agent Tools)."""

    async def list_tables(self, schema: str = "public") -> List[str]:
        """List all available tables."""
        ...

    async def get_table_definition(self, table_name: str) -> str:
        """Get a string representation of the table schema (DDL or JSON)."""
        ...


@runtime_checkable
class ExtendedVectorIndex(Protocol):
    """Extended VectorIndex protocol with structured filtering support.

    This extends the base VectorIndex with:
    - Structured filter support (FilterCriteria instead of raw strings)
    - String IDs for cross-backend compatibility (Pinecone requires client-side IDs)
    - Metadata support for richer item storage

    Note: The base VectorIndex in vector_indexes/protocol.py remains unchanged
    for backward compatibility. Use this protocol for new implementations
    that need filtering or metadata support.
    """

    def search(
        self,
        query_vector: np.ndarray,
        k: int,
        filter: Optional[FilterCriteria] = None,
    ) -> List[SearchResult]:
        """Search for k nearest neighbors with optional filtering.

        Args:
            query_vector: 1D numpy array of the query embedding.
            k: Number of neighbors to return.
            filter: Optional structured filter criteria.

        Returns:
            List of SearchResult sorted by score descending.
        """
        ...

    def add_items(
        self,
        vectors: np.ndarray,
        ids: List[str],
        metadata: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        """Add items with explicit string IDs.

        String IDs are required for Pinecone and other cloud vector DBs
        that require client-side ID generation.

        Args:
            vectors: 2D numpy array of shape (n_items, dimension).
            ids: List of unique string identifiers for each vector.
            metadata: Optional list of metadata dicts for each vector.
        """
        ...

    def delete_items(self, ids: List[str]) -> None:
        """Delete items by their IDs.

        Args:
            ids: List of item IDs to delete.
        """
        ...

    def save(self, path: str) -> None:
        """Persist the index to disk.

        Args:
            path: File path to save the index.
        """
        ...

    def load(self, path: str) -> None:
        """Load the index from disk.

        Args:
            path: File path to load the index from.
        """
        ...

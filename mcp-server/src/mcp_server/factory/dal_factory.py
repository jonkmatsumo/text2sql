from typing import Tuple

from mcp_server.dal.interfaces.cache_store import CacheStore
from mcp_server.dal.interfaces.example_store import ExampleStore
from mcp_server.dal.interfaces.graph_store import GraphStore
from mcp_server.dal.interfaces.metadata_store import MetadataStore
from mcp_server.dal.interfaces.schema_introspector import SchemaIntrospector
from mcp_server.dal.interfaces.schema_store import SchemaStore
from mcp_server.dal.memgraph.graph_store import MemgraphStore
from mcp_server.dal.postgres.example_store import PostgresExampleStore
from mcp_server.dal.postgres.metadata_store import PostgresMetadataStore
from mcp_server.dal.postgres.schema_introspector import PostgresSchemaIntrospector
from mcp_server.dal.postgres.schema_store import PostgresSchemaStore
from mcp_server.dal.postgres.semantic_cache import PgSemanticCache


class DALFactory:
    """Factory for creating Data Access Layer components."""

    @staticmethod
    def create_graph_store(uri: str, user: str, password: str) -> GraphStore:
        """Create a Memgraph graph store."""
        return MemgraphStore(uri, user, password)

    @staticmethod
    def create_stores() -> (
        Tuple[CacheStore, ExampleStore, SchemaStore, SchemaIntrospector, MetadataStore]
    ):
        """
        Create and return all PostgreSQL-based stores.

        Returns:
            Tuple of (CacheStore, ExampleStore, SchemaStore, SchemaIntrospector, MetadataStore)
        """
        # In the future, this could read env vars to decide which impl to return
        return (
            PgSemanticCache(),
            PostgresExampleStore(),
            PostgresSchemaStore(),
            PostgresSchemaIntrospector(),
            PostgresMetadataStore(),
        )

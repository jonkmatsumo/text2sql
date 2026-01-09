from typing import Tuple

from mcp_server.dal.interfaces import (
    CacheStore,
    ExampleStore,
    GraphStore,
    MetadataStore,
    SchemaIntrospector,
    SchemaStore,
)
from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.dal.postgres import (
    PgSemanticCache,
    PostgresExampleStore,
    PostgresMetadataStore,
    PostgresSchemaIntrospector,
    PostgresSchemaStore,
)


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

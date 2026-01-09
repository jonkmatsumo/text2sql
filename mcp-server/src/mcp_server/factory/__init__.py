"""Factory package for MCP server components.

NOTE: Factory functions have been moved to mcp_server.dal.factory.

Available factory functions (import from mcp_server.dal.factory):
    - get_retriever() -> DataSchemaRetriever
    - get_cache_store() -> CacheStore
    - get_example_store() -> ExampleStore
    - get_schema_store() -> SchemaStore
    - get_schema_introspector() -> SchemaIntrospector
    - get_metadata_store() -> MetadataStore
    - get_graph_store() -> GraphStore

This package contains only the provider helpers module (providers.py).
"""

__all__: list[str] = []

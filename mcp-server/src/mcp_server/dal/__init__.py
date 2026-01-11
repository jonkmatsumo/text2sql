"""Data Abstraction Layer (DAL) for the MCP Server.

This package exposes the core interfaces and implementation-agnostic contexts for data access,
including the Function Call Context for tenant isolation.
"""

from mcp_server.utils.tenant_context import (
    get_current_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_context,
)

from .interfaces.cache_store import CacheStore
from .interfaces.example_store import ExampleStore
from .interfaces.extended_vector_index import ExtendedVectorIndex
from .interfaces.graph_store import GraphStore
from .interfaces.metadata_store import MetadataStore
from .interfaces.schema_introspector import SchemaIntrospector
from .interfaces.schema_store import SchemaStore

__all__ = [
    "get_current_tenant",
    "reset_tenant_context",
    "set_current_tenant",
    "tenant_context",
    "CacheStore",
    "ExampleStore",
    "ExtendedVectorIndex",
    "GraphStore",
    "MetadataStore",
    "SchemaIntrospector",
    "SchemaStore",
]

"""DAL Interfaces (Protocols).

This package contains the Protocol definitions for the Data Abstraction Layer components.
"""

from .cache_store import CacheStore
from .example_store import ExampleStore
from .extended_vector_index import ExtendedVectorIndex
from .graph_store import GraphStore
from .metadata_store import MetadataStore
from .registry_store import RegistryStore
from .schema_introspector import SchemaIntrospector
from .schema_store import SchemaStore

__all__ = [
    "CacheStore",
    "ExampleStore",
    "ExtendedVectorIndex",
    "GraphStore",
    "MetadataStore",
    "RegistryStore",
    "SchemaIntrospector",
    "SchemaStore",
]

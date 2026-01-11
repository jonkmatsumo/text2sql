"""DAL Factory with singleton, environment-driven provider selection.

This module provides lazy singleton getters for DAL components.
Provider selection is controlled via environment variables.

Environment Variables:
    GRAPH_STORE_PROVIDER: Provider for GraphStore (default: "memgraph")
    CACHE_STORE_PROVIDER: Provider for CacheStore (default: "postgres")
    EXAMPLE_STORE_PROVIDER: Provider for ExampleStore (default: "postgres")
    SCHEMA_STORE_PROVIDER: Provider for SchemaStore (default: "postgres")
    SCHEMA_INTROSPECTOR_PROVIDER: Provider for SchemaIntrospector (default: "postgres")
    METADATA_STORE_PROVIDER: Provider for MetadataStore (default: "postgres")

Canonical Provider IDs:
    - "postgres": PostgreSQL-based implementations
    - "memgraph": Memgraph/Neo4j-based implementations

Example:
    >>> from mcp_server.dal.factory import get_cache_store, get_graph_store
    >>> cache = get_cache_store()  # Returns PgSemanticCache by default
    >>> graph = get_graph_store()  # Returns MemgraphStore by default
"""

import logging
import os
from typing import Optional

from mcp_server.dal.interfaces import (
    CacheStore,
    ConversationStore,
    ExampleStore,
    FeedbackStore,
    GraphStore,
    InteractionStore,
    MetadataStore,
    RegistryStore,
    SchemaIntrospector,
    SchemaStore,
)
from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.dal.postgres import (
    PgSemanticCache,
    PostgresConversationStore,
    PostgresExampleStore,
    PostgresFeedbackStore,
    PostgresInteractionStore,
    PostgresMetadataStore,
    PostgresRegistryStore,
    PostgresSchemaIntrospector,
    PostgresSchemaStore,
)
from mcp_server.dal.util.env import get_provider_env

logger = logging.getLogger(__name__)

# =============================================================================
# Provider Registries
# =============================================================================

GRAPH_STORE_PROVIDERS: dict[str, type[GraphStore]] = {
    "memgraph": MemgraphStore,
}

CACHE_STORE_PROVIDERS: dict[str, type[CacheStore]] = {
    "postgres": PgSemanticCache,
}

EXAMPLE_STORE_PROVIDERS: dict[str, type[ExampleStore]] = {
    "postgres": PostgresExampleStore,
}

SCHEMA_STORE_PROVIDERS: dict[str, type[SchemaStore]] = {
    "postgres": PostgresSchemaStore,
}

SCHEMA_INTROSPECTOR_PROVIDERS: dict[str, type[SchemaIntrospector]] = {
    "postgres": PostgresSchemaIntrospector,
}

METADATA_STORE_PROVIDERS: dict[str, type[MetadataStore]] = {
    "postgres": PostgresMetadataStore,
}

REGISTRY_STORE_PROVIDERS: dict[str, type[RegistryStore]] = {
    "postgres": PostgresRegistryStore,
}

CONVERSATION_STORE_PROVIDERS: dict[str, type[ConversationStore]] = {
    "postgres": PostgresConversationStore,
}

FEEDBACK_STORE_PROVIDERS: dict[str, type[FeedbackStore]] = {
    "postgres": PostgresFeedbackStore,
}

INTERACTION_STORE_PROVIDERS: dict[str, type[InteractionStore]] = {
    "postgres": PostgresInteractionStore,
}


# =============================================================================
# Singleton Instances
# =============================================================================

_graph_store: Optional[GraphStore] = None
_cache_store: Optional[CacheStore] = None
_example_store: Optional[ExampleStore] = None
_schema_store: Optional[SchemaStore] = None
_schema_introspector: Optional[SchemaIntrospector] = None
_metadata_store: Optional[MetadataStore] = None
_registry_store: Optional[RegistryStore] = None
_conversation_store: Optional[ConversationStore] = None
_feedback_store: Optional[FeedbackStore] = None
_interaction_store: Optional[InteractionStore] = None


# =============================================================================
# Singleton Getters
# =============================================================================


def get_graph_store() -> GraphStore:
    """Get or create the singleton GraphStore instance.

    Provider is selected via GRAPH_STORE_PROVIDER env var.
    Default: "memgraph" (MemgraphStore)

    MemgraphStore reads connection details from environment:
        - MEMGRAPH_URI (default: "bolt://localhost:7687")
        - MEMGRAPH_USER (default: "")
        - MEMGRAPH_PASSWORD (default: "")

    Returns:
        The singleton GraphStore instance.

    Raises:
        ValueError: If GRAPH_STORE_PROVIDER is set to an invalid value.
    """
    global _graph_store
    if _graph_store is None:
        provider = get_provider_env(
            "GRAPH_STORE_PROVIDER",
            default="memgraph",
            allowed=set(GRAPH_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing GraphStore with provider: {provider}")

        # MemgraphStore requires connection params from environment
        uri = os.environ.get("MEMGRAPH_URI", "bolt://localhost:7687")
        user = os.environ.get("MEMGRAPH_USER", "")
        password = os.environ.get("MEMGRAPH_PASSWORD", "")

        store_cls = GRAPH_STORE_PROVIDERS[provider]
        _graph_store = store_cls(uri, user, password)

    return _graph_store


def get_cache_store() -> CacheStore:
    """Get or create the singleton CacheStore instance.

    Provider is selected via CACHE_STORE_PROVIDER env var.
    Default: "postgres" (PgSemanticCache)

    Returns:
        The singleton CacheStore instance.

    Raises:
        ValueError: If CACHE_STORE_PROVIDER is set to an invalid value.
    """
    global _cache_store
    if _cache_store is None:
        provider = get_provider_env(
            "CACHE_STORE_PROVIDER",
            default="postgres",
            allowed=set(CACHE_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing CacheStore with provider: {provider}")

        store_cls = CACHE_STORE_PROVIDERS[provider]
        _cache_store = store_cls()

    return _cache_store


def get_example_store() -> ExampleStore:
    """Get or create the singleton ExampleStore instance.

    Provider is selected via EXAMPLE_STORE_PROVIDER env var.
    Default: "postgres" (PostgresExampleStore)

    Returns:
        The singleton ExampleStore instance.

    Raises:
        ValueError: If EXAMPLE_STORE_PROVIDER is set to an invalid value.
    """
    global _example_store
    if _example_store is None:
        provider = get_provider_env(
            "EXAMPLE_STORE_PROVIDER",
            default="postgres",
            allowed=set(EXAMPLE_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing ExampleStore with provider: {provider}")

        store_cls = EXAMPLE_STORE_PROVIDERS[provider]
        _example_store = store_cls()

    return _example_store


def get_registry_store() -> RegistryStore:
    """Get or create the singleton RegistryStore instance.

    Provider is selected via REGISTRY_STORE_PROVIDER env var.
    Default: "postgres" (PostgresRegistryStore)

    Returns:
        The singleton RegistryStore instance.
    """
    global _registry_store
    if _registry_store is None:
        provider = get_provider_env(
            "REGISTRY_STORE_PROVIDER",
            default="postgres",
            allowed=set(REGISTRY_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing RegistryStore with provider: {provider}")

        store_cls = REGISTRY_STORE_PROVIDERS[provider]
        _registry_store = store_cls()

    return _registry_store


def get_schema_store() -> SchemaStore:
    """Get or create the singleton SchemaStore instance.

    Provider is selected via SCHEMA_STORE_PROVIDER env var.
    Default: "postgres" (PostgresSchemaStore)

    Returns:
        The singleton SchemaStore instance.

    Raises:
        ValueError: If SCHEMA_STORE_PROVIDER is set to an invalid value.
    """
    global _schema_store
    if _schema_store is None:
        provider = get_provider_env(
            "SCHEMA_STORE_PROVIDER",
            default="postgres",
            allowed=set(SCHEMA_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing SchemaStore with provider: {provider}")

        store_cls = SCHEMA_STORE_PROVIDERS[provider]
        _schema_store = store_cls()

    return _schema_store


def get_schema_introspector() -> SchemaIntrospector:
    """Get or create the singleton SchemaIntrospector instance.

    Provider is selected via SCHEMA_INTROSPECTOR_PROVIDER env var.
    Default: "postgres" (PostgresSchemaIntrospector)

    Returns:
        The singleton SchemaIntrospector instance.

    Raises:
        ValueError: If SCHEMA_INTROSPECTOR_PROVIDER is set to an invalid value.
    """
    global _schema_introspector
    if _schema_introspector is None:
        provider = get_provider_env(
            "SCHEMA_INTROSPECTOR_PROVIDER",
            default="postgres",
            allowed=set(SCHEMA_INTROSPECTOR_PROVIDERS.keys()),
        )
        logger.info(f"Initializing SchemaIntrospector with provider: {provider}")

        store_cls = SCHEMA_INTROSPECTOR_PROVIDERS[provider]
        _schema_introspector = store_cls()

    return _schema_introspector


def get_metadata_store() -> MetadataStore:
    """Get or create the singleton MetadataStore instance.

    Provider is selected via METADATA_STORE_PROVIDER env var.
    Default: "postgres" (PostgresMetadataStore)

    Returns:
        The singleton MetadataStore instance.

    Raises:
        ValueError: If METADATA_STORE_PROVIDER is set to an invalid value.
    """
    global _metadata_store
    if _metadata_store is None:
        provider = get_provider_env(
            "METADATA_STORE_PROVIDER",
            default="postgres",
            allowed=set(METADATA_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing MetadataStore with provider: {provider}")

        store_cls = METADATA_STORE_PROVIDERS[provider]
        _metadata_store = store_cls()

    return _metadata_store


# =============================================================================
# Testing Utilities
# =============================================================================


def reset_singletons() -> None:
    """Reset all singleton instances (for testing only).

    This allows tests to reinitialize stores with different providers.
    Should not be called in production code.
    """
    global _graph_store, _cache_store, _example_store, _registry_store
    global _schema_store, _schema_introspector, _metadata_store
    global _conversation_store, _feedback_store, _interaction_store

    _graph_store = None
    _cache_store = None
    _example_store = None
    _schema_store = None
    _schema_introspector = None
    _metadata_store = None
    _registry_store = None
    _conversation_store = None
    _feedback_store = None
    _interaction_store = None


def get_conversation_store() -> ConversationStore:
    """Get or create the singleton ConversationStore instance."""
    global _conversation_store
    if _conversation_store is None:
        provider = get_provider_env(
            "CONVERSATION_STORE_PROVIDER",
            default="postgres",
            allowed=set(CONVERSATION_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing ConversationStore with provider: {provider}")
        store_cls = CONVERSATION_STORE_PROVIDERS[provider]
        _conversation_store = store_cls()
    return _conversation_store


def get_feedback_store() -> FeedbackStore:
    """Get or create the singleton FeedbackStore instance."""
    global _feedback_store
    if _feedback_store is None:
        provider = get_provider_env(
            "FEEDBACK_STORE_PROVIDER",
            default="postgres",
            allowed=set(FEEDBACK_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing FeedbackStore with provider: {provider}")
        store_cls = FEEDBACK_STORE_PROVIDERS[provider]
        _feedback_store = store_cls()
    return _feedback_store


def get_interaction_store() -> InteractionStore:
    """Get or create the singleton InteractionStore instance."""
    global _interaction_store
    if _interaction_store is None:
        provider = get_provider_env(
            "INTERACTION_STORE_PROVIDER",
            default="postgres",
            allowed=set(INTERACTION_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing InteractionStore with provider: {provider}")
        store_cls = INTERACTION_STORE_PROVIDERS[provider]
        _interaction_store = store_cls()
    return _interaction_store

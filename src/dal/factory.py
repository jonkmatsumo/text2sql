"""DAL Factory with singleton, environment-driven provider selection.

This module provides lazy singleton getters for DAL components.
Provider selection is controlled via environment variables.

Environment Variables:
    GRAPH_STORE_PROVIDER: Provider for GraphStore (default: "memgraph")
    CACHE_STORE_PROVIDER: Provider for CacheStore (default: "postgres")
    EXAMPLE_STORE_PROVIDER: Provider for ExampleStore (default: "postgres")
    SCHEMA_STORE_PROVIDER: Provider for SchemaStore (default: "postgres")
    SCHEMA_INTROSPECTOR_PROVIDER: Provider for SchemaIntrospector (default: "postgres")
    SCHEMA_INTROSPECTOR_PROVIDER: Provider for SchemaIntrospector (default: "postgres")
    METADATA_STORE_PROVIDER: Provider for MetadataStore (default: "postgres")
    PATTERN_RUN_STORE_PROVIDER: Provider for PatternRunStore (default: "postgres")

Canonical Provider IDs:
    - "postgres": PostgreSQL-based implementations
    - "memgraph": Memgraph/Neo4j-based implementations

Example:
    >>> from dal.factory import get_cache_store, get_graph_store
    >>> cache = get_cache_store()  # Returns PgSemanticCache by default
    >>> graph = get_graph_store()  # Returns MemgraphStore by default
"""

import logging
from typing import Optional

from common.interfaces import (
    CacheStore,
    ConversationStore,
    EvaluationStore,
    ExampleStore,
    FeedbackStore,
    GraphStore,
    InteractionStore,
    MetadataStore,
    PatternRunStore,
    RegistryStore,
    SchemaIntrospector,
    SchemaStore,
    SynthRunStore,
)
from dal.util.env import get_provider_env

logger = logging.getLogger(__name__)

# =============================================================================
# Provider Registries
# =============================================================================

GRAPH_STORE_PROVIDERS: "dict[str, type[GraphStore]]" = {}
CACHE_STORE_PROVIDERS: "dict[str, type[CacheStore]]" = {}
EXAMPLE_STORE_PROVIDERS: "dict[str, type[ExampleStore]]" = {}
SCHEMA_STORE_PROVIDERS: "dict[str, type[SchemaStore]]" = {}
SCHEMA_INTROSPECTOR_PROVIDERS: "dict[str, type[SchemaIntrospector]]" = {}
METADATA_STORE_PROVIDERS: "dict[str, type[MetadataStore]]" = {}
PATTERN_RUN_STORE_PROVIDERS: "dict[str, type[PatternRunStore]]" = {}
REGISTRY_STORE_PROVIDERS: "dict[str, type[RegistryStore]]" = {}
SYNTH_RUN_STORE_PROVIDERS: "dict[str, type[SynthRunStore]]" = {}
CONVERSATION_STORE_PROVIDERS: "dict[str, type[ConversationStore]]" = {}
FEEDBACK_STORE_PROVIDERS: "dict[str, type[FeedbackStore]]" = {}
INTERACTION_STORE_PROVIDERS: "dict[str, type[InteractionStore]]" = {}
EVALUATION_STORE_PROVIDERS: "dict[str, type[EvaluationStore]]" = {}


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
_pattern_run_store: Optional[PatternRunStore] = None
_synth_run_store: Optional[SynthRunStore] = None


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
    global _graph_store, GRAPH_STORE_PROVIDERS
    if _graph_store is None:
        # Import implementations lazily to avoid import loops and expensive init
        if "memgraph" not in GRAPH_STORE_PROVIDERS:
            from dal.memgraph import MemgraphStore

            GRAPH_STORE_PROVIDERS["memgraph"] = MemgraphStore

        provider = get_provider_env(
            "GRAPH_STORE_PROVIDER",
            default="memgraph",
            allowed=set(GRAPH_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing GraphStore with provider: {provider}")

        from common.config.env import get_env_str

        # MemgraphStore requires connection params from environment
        uri = get_env_str("MEMGRAPH_URI", "bolt://localhost:7687")
        user = get_env_str("MEMGRAPH_USER", "")
        password = get_env_str("MEMGRAPH_PASSWORD", "")

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
    global _cache_store, CACHE_STORE_PROVIDERS
    if _cache_store is None:
        if "postgres" not in CACHE_STORE_PROVIDERS:
            from dal.postgres import PgSemanticCache

            CACHE_STORE_PROVIDERS["postgres"] = PgSemanticCache

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
    global _example_store, EXAMPLE_STORE_PROVIDERS
    if _example_store is None:
        if "postgres" not in EXAMPLE_STORE_PROVIDERS:
            from dal.postgres import PostgresExampleStore

            EXAMPLE_STORE_PROVIDERS["postgres"] = PostgresExampleStore

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
    global _registry_store, REGISTRY_STORE_PROVIDERS
    if _registry_store is None:
        if "postgres" not in REGISTRY_STORE_PROVIDERS:
            from dal.postgres import PostgresRegistryStore

            REGISTRY_STORE_PROVIDERS["postgres"] = PostgresRegistryStore

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
    global _schema_store, SCHEMA_STORE_PROVIDERS
    if _schema_store is None:
        if "postgres" not in SCHEMA_STORE_PROVIDERS:
            from dal.postgres import PostgresSchemaStore

            SCHEMA_STORE_PROVIDERS["postgres"] = PostgresSchemaStore

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
    global _schema_introspector, SCHEMA_INTROSPECTOR_PROVIDERS
    if _schema_introspector is None:
        if "postgres" not in SCHEMA_INTROSPECTOR_PROVIDERS:
            from dal.postgres import PostgresSchemaIntrospector

            SCHEMA_INTROSPECTOR_PROVIDERS["postgres"] = PostgresSchemaIntrospector

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
    global _metadata_store, METADATA_STORE_PROVIDERS
    if _metadata_store is None:
        if "postgres" not in METADATA_STORE_PROVIDERS:
            from dal.postgres import PostgresMetadataStore

            METADATA_STORE_PROVIDERS["postgres"] = PostgresMetadataStore

        provider = get_provider_env(
            "METADATA_STORE_PROVIDER",
            default="postgres",
            allowed=set(METADATA_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing MetadataStore with provider: {provider}")

        store_cls = METADATA_STORE_PROVIDERS[provider]
        _metadata_store = store_cls()

    return _metadata_store


def get_pattern_run_store() -> PatternRunStore:
    """Get or create the singleton PatternRunStore instance.

    Provider is selected via PATTERN_RUN_STORE_PROVIDER env var.
    Default: "postgres" (PostgresPatternRunStore)

    Returns:
        The singleton PatternRunStore instance.
    """
    global _pattern_run_store, PATTERN_RUN_STORE_PROVIDERS
    if _pattern_run_store is None:
        if "postgres" not in PATTERN_RUN_STORE_PROVIDERS:
            from dal.postgres import PostgresPatternRunStore

            PATTERN_RUN_STORE_PROVIDERS["postgres"] = PostgresPatternRunStore

        provider = get_provider_env(
            "PATTERN_RUN_STORE_PROVIDER",
            default="postgres",
            allowed=set(PATTERN_RUN_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing PatternRunStore with provider: {provider}")

        store_cls = PATTERN_RUN_STORE_PROVIDERS[provider]
        _pattern_run_store = store_cls()

    return _pattern_run_store


def get_synth_run_store() -> SynthRunStore:
    """Get or create the singleton SynthRunStore instance.

    Provider is selected via SYNTH_RUN_STORE_PROVIDER env var.
    Default: "postgres" (PostgresSynthRunStore)

    Returns:
        The singleton SynthRunStore instance.
    """
    global _synth_run_store, SYNTH_RUN_STORE_PROVIDERS
    if _synth_run_store is None:
        if "postgres" not in SYNTH_RUN_STORE_PROVIDERS:
            from dal.postgres import PostgresSynthRunStore

            SYNTH_RUN_STORE_PROVIDERS["postgres"] = PostgresSynthRunStore

        provider = get_provider_env(
            "SYNTH_RUN_STORE_PROVIDER",
            default="postgres",
            allowed=set(SYNTH_RUN_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing SynthRunStore with provider: {provider}")

        store_cls = SYNTH_RUN_STORE_PROVIDERS[provider]
        _synth_run_store = store_cls()

    return _synth_run_store


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
    global _conversation_store, _feedback_store, _interaction_store, _pattern_run_store
    global _evaluation_store, _synth_run_store

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
    _interaction_store = None
    _pattern_run_store = None
    _evaluation_store = None
    _synth_run_store = None


def get_conversation_store() -> ConversationStore:
    """Get or create the singleton ConversationStore instance."""
    global _conversation_store, CONVERSATION_STORE_PROVIDERS
    if _conversation_store is None:
        if "postgres" not in CONVERSATION_STORE_PROVIDERS:
            from dal.postgres import PostgresConversationStore

            CONVERSATION_STORE_PROVIDERS["postgres"] = PostgresConversationStore

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
    global _feedback_store, FEEDBACK_STORE_PROVIDERS
    if _feedback_store is None:
        if "postgres" not in FEEDBACK_STORE_PROVIDERS:
            from dal.postgres import PostgresFeedbackStore

            FEEDBACK_STORE_PROVIDERS["postgres"] = PostgresFeedbackStore

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
    global _interaction_store, INTERACTION_STORE_PROVIDERS
    if _interaction_store is None:
        if "postgres" not in INTERACTION_STORE_PROVIDERS:
            from dal.postgres import PostgresInteractionStore

            INTERACTION_STORE_PROVIDERS["postgres"] = PostgresInteractionStore

        provider = get_provider_env(
            "INTERACTION_STORE_PROVIDER",
            default="postgres",
            allowed=set(INTERACTION_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing InteractionStore with provider: {provider}")
        store_cls = INTERACTION_STORE_PROVIDERS[provider]
        _interaction_store = store_cls()
    return _interaction_store


# =============================================================================
# Evaluation Store
# =============================================================================


_evaluation_store: Optional[EvaluationStore] = None


def get_evaluation_store() -> EvaluationStore:
    """Get or create the singleton EvaluationStore instance.

    Provider is selected via EVALUATION_STORE_PROVIDER env var.
    Default: "postgres" (PostgresEvaluationStore)

    Returns:
        The singleton EvaluationStore instance.
    """
    global _evaluation_store, EVALUATION_STORE_PROVIDERS
    if _evaluation_store is None:
        if "postgres" not in EVALUATION_STORE_PROVIDERS:
            from dal.postgres import PostgresEvaluationStore

            EVALUATION_STORE_PROVIDERS["postgres"] = PostgresEvaluationStore

        provider = get_provider_env(
            "EVALUATION_STORE_PROVIDER",
            default="postgres",
            allowed=set(EVALUATION_STORE_PROVIDERS.keys()),
        )
        logger.info(f"Initializing EvaluationStore with provider: {provider}")
        store_cls = EVALUATION_STORE_PROVIDERS[provider]
        _evaluation_store = store_cls()
    return _evaluation_store

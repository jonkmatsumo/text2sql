"""Unit tests for DAL factory singleton getters."""

import os
from unittest.mock import MagicMock, patch

import pytest
from mcp_server.dal.factory import (
    CACHE_STORE_PROVIDERS,
    EXAMPLE_STORE_PROVIDERS,
    GRAPH_STORE_PROVIDERS,
    METADATA_STORE_PROVIDERS,
    SCHEMA_INTROSPECTOR_PROVIDERS,
    SCHEMA_STORE_PROVIDERS,
    get_cache_store,
    get_example_store,
    get_graph_store,
    get_metadata_store,
    get_schema_introspector,
    get_schema_store,
    reset_singletons,
)
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


@pytest.fixture(autouse=True)
def reset_factory_state():
    """Reset singleton state before and after each test."""
    reset_singletons()
    yield
    reset_singletons()


class TestProviderRegistries:
    """Tests for provider registry definitions."""

    def test_graph_store_providers_contains_memgraph(self):
        """Verify Memgraph is registered for graph stores."""
        assert "memgraph" in GRAPH_STORE_PROVIDERS
        assert GRAPH_STORE_PROVIDERS["memgraph"] is MemgraphStore

    def test_cache_store_providers_contains_postgres(self):
        """Verify Postgres is registered for cache stores."""
        assert "postgres" in CACHE_STORE_PROVIDERS
        assert CACHE_STORE_PROVIDERS["postgres"] is PgSemanticCache

    def test_example_store_providers_contains_postgres(self):
        """Verify Postgres is registered for example stores."""
        assert "postgres" in EXAMPLE_STORE_PROVIDERS
        assert EXAMPLE_STORE_PROVIDERS["postgres"] is PostgresExampleStore

    def test_schema_store_providers_contains_postgres(self):
        """Verify Postgres is registered for schema stores."""
        assert "postgres" in SCHEMA_STORE_PROVIDERS
        assert SCHEMA_STORE_PROVIDERS["postgres"] is PostgresSchemaStore

    def test_schema_introspector_providers_contains_postgres(self):
        """Verify Postgres is registered for schema introspectors."""
        assert "postgres" in SCHEMA_INTROSPECTOR_PROVIDERS
        assert SCHEMA_INTROSPECTOR_PROVIDERS["postgres"] is PostgresSchemaIntrospector

    def test_metadata_store_providers_contains_postgres(self):
        """Verify Postgres is registered for metadata stores."""
        assert "postgres" in METADATA_STORE_PROVIDERS
        assert METADATA_STORE_PROVIDERS["postgres"] is PostgresMetadataStore


class TestDefaultProviders:
    """Tests for default provider behavior (no env vars set)."""

    def test_cache_store_defaults_to_postgres(self):
        """Test that CacheStore defaults to PgSemanticCache."""
        store = get_cache_store()
        assert isinstance(store, PgSemanticCache)

    def test_example_store_defaults_to_postgres(self):
        """Test that ExampleStore defaults to PostgresExampleStore."""
        store = get_example_store()
        assert isinstance(store, PostgresExampleStore)

    def test_schema_store_defaults_to_postgres(self):
        """Test that SchemaStore defaults to PostgresSchemaStore."""
        store = get_schema_store()
        assert isinstance(store, PostgresSchemaStore)

    def test_schema_introspector_defaults_to_postgres(self):
        """Test that SchemaIntrospector defaults to PostgresSchemaIntrospector."""
        store = get_schema_introspector()
        assert isinstance(store, PostgresSchemaIntrospector)

    def test_metadata_store_defaults_to_postgres(self):
        """Test that MetadataStore defaults to PostgresMetadataStore."""
        store = get_metadata_store()
        assert isinstance(store, PostgresMetadataStore)


class TestGraphStoreProvider:
    """Tests for GraphStore provider selection."""

    def test_graph_store_defaults_to_memgraph(self):
        """Test that GraphStore defaults to MemgraphStore type."""
        # Create a mock class that we'll use in the registry
        mock_cls = MagicMock(return_value=MagicMock(spec=GraphStore))

        with patch.dict("mcp_server.dal.factory.GRAPH_STORE_PROVIDERS", {"memgraph": mock_cls}):
            store = get_graph_store()

        mock_cls.assert_called_once()
        assert store is mock_cls.return_value

    def test_graph_store_uses_env_connection_params(self):
        """Test that GraphStore reads connection params from environment."""
        mock_cls = MagicMock(return_value=MagicMock(spec=GraphStore))

        env_vars = {
            "MEMGRAPH_URI": "bolt://custom:7687",
            "MEMGRAPH_USER": "testuser",
            "MEMGRAPH_PASSWORD": "testpass",
        }

        with patch.dict(os.environ, env_vars):
            with patch.dict("mcp_server.dal.factory.GRAPH_STORE_PROVIDERS", {"memgraph": mock_cls}):
                get_graph_store()

        mock_cls.assert_called_once_with("bolt://custom:7687", "testuser", "testpass")

    def test_graph_store_uses_default_connection_params(self):
        """Test that GraphStore uses defaults when env vars not set."""
        mock_cls = MagicMock(return_value=MagicMock(spec=GraphStore))

        # Clear env vars but keep provider default working
        with patch.dict(
            os.environ,
            {"MEMGRAPH_URI": "", "MEMGRAPH_USER": "", "MEMGRAPH_PASSWORD": ""},
            clear=False,
        ):
            # Remove the env vars entirely
            for key in ["MEMGRAPH_URI", "MEMGRAPH_USER", "MEMGRAPH_PASSWORD"]:
                os.environ.pop(key, None)

            with patch.dict("mcp_server.dal.factory.GRAPH_STORE_PROVIDERS", {"memgraph": mock_cls}):
                get_graph_store()

        mock_cls.assert_called_once_with("bolt://localhost:7687", "", "")


class TestSingletonBehavior:
    """Tests for singleton pattern implementation."""

    def test_cache_store_is_singleton(self):
        """get_cache_store returns same instance on multiple calls."""
        store1 = get_cache_store()
        store2 = get_cache_store()
        assert store1 is store2

    def test_example_store_is_singleton(self):
        """get_example_store returns same instance on multiple calls."""
        store1 = get_example_store()
        store2 = get_example_store()
        assert store1 is store2

    def test_schema_store_is_singleton(self):
        """get_schema_store returns same instance on multiple calls."""
        store1 = get_schema_store()
        store2 = get_schema_store()
        assert store1 is store2

    def test_schema_introspector_is_singleton(self):
        """get_schema_introspector returns same instance on multiple calls."""
        store1 = get_schema_introspector()
        store2 = get_schema_introspector()
        assert store1 is store2

    def test_metadata_store_is_singleton(self):
        """get_metadata_store returns same instance on multiple calls."""
        store1 = get_metadata_store()
        store2 = get_metadata_store()
        assert store1 is store2

    def test_graph_store_is_singleton(self):
        """get_graph_store returns same instance on multiple calls."""
        mock_instance = MagicMock(spec=GraphStore)
        mock_cls = MagicMock(return_value=mock_instance)

        with patch.dict("mcp_server.dal.factory.GRAPH_STORE_PROVIDERS", {"memgraph": mock_cls}):
            store1 = get_graph_store()
            store2 = get_graph_store()

        assert store1 is store2
        # Should only be called once due to singleton
        mock_cls.assert_called_once()


class TestResetSingletons:
    """Tests for reset_singletons utility."""

    def test_reset_allows_reinitialization(self):
        """reset_singletons allows new instances to be created."""
        store1 = get_cache_store()
        reset_singletons()
        store2 = get_cache_store()

        # Should be different instances after reset
        assert store1 is not store2

    def test_reset_allows_graph_store_reinitialization(self):
        """reset_singletons allows GraphStore reinitialization."""
        mock_instance1 = MagicMock(spec=GraphStore)
        mock_instance2 = MagicMock(spec=GraphStore)
        call_count = [0]

        def create_instance(*args, **kwargs):
            call_count[0] += 1
            return mock_instance1 if call_count[0] == 1 else mock_instance2

        mock_cls = MagicMock(side_effect=create_instance)

        with patch.dict("mcp_server.dal.factory.GRAPH_STORE_PROVIDERS", {"memgraph": mock_cls}):
            store1 = get_graph_store()
            reset_singletons()
            store2 = get_graph_store()

        assert store1 is not store2
        assert mock_cls.call_count == 2


class TestEnvVarProviderSelection:
    """Tests for environment variable provider selection."""

    def test_invalid_cache_provider_raises_error(self):
        """Invalid CACHE_STORE_PROVIDER raises ValueError."""
        with patch.dict(os.environ, {"CACHE_STORE_PROVIDER": "invalid"}):
            with pytest.raises(ValueError) as exc_info:
                get_cache_store()

            assert "CACHE_STORE_PROVIDER" in str(exc_info.value)
            assert "invalid" in str(exc_info.value)

    def test_invalid_example_provider_raises_error(self):
        """Invalid EXAMPLE_STORE_PROVIDER raises ValueError."""
        with patch.dict(os.environ, {"EXAMPLE_STORE_PROVIDER": "invalid"}):
            with pytest.raises(ValueError) as exc_info:
                get_example_store()

            assert "EXAMPLE_STORE_PROVIDER" in str(exc_info.value)

    def test_invalid_graph_provider_raises_error(self):
        """Invalid GRAPH_STORE_PROVIDER raises ValueError."""
        with patch.dict(os.environ, {"GRAPH_STORE_PROVIDER": "invalid"}):
            with pytest.raises(ValueError) as exc_info:
                get_graph_store()

            assert "GRAPH_STORE_PROVIDER" in str(exc_info.value)

    def test_provider_alias_postgresql_works(self):
        """Verify PostgreSQL alias is accepted for Postgres stores."""
        with patch.dict(os.environ, {"CACHE_STORE_PROVIDER": "PostgreSQL"}):
            store = get_cache_store()
            assert isinstance(store, PgSemanticCache)

    def test_provider_alias_pg_works(self):
        """Verify PG alias is accepted for Postgres stores."""
        with patch.dict(os.environ, {"EXAMPLE_STORE_PROVIDER": "PG"}):
            store = get_example_store()
            assert isinstance(store, PostgresExampleStore)


class TestInterfaceCompliance:
    """Tests verifying stores implement correct interfaces."""

    def test_cache_store_implements_interface(self):
        """Test that CacheStore getter returns CacheStore protocol."""
        store = get_cache_store()
        assert isinstance(store, CacheStore)

    def test_example_store_implements_interface(self):
        """Test that ExampleStore getter returns ExampleStore protocol."""
        store = get_example_store()
        assert isinstance(store, ExampleStore)

    def test_schema_store_implements_interface(self):
        """Test that SchemaStore getter returns SchemaStore protocol."""
        store = get_schema_store()
        assert isinstance(store, SchemaStore)

    def test_schema_introspector_implements_interface(self):
        """Test that SchemaIntrospector getter returns SchemaIntrospector protocol."""
        store = get_schema_introspector()
        assert isinstance(store, SchemaIntrospector)

    def test_metadata_store_implements_interface(self):
        """Test that MetadataStore getter returns MetadataStore protocol."""
        store = get_metadata_store()
        assert isinstance(store, MetadataStore)

"""Unit tests for provider normalization and environment helpers."""

import os
from unittest.mock import patch

import pytest
from mcp_server.dal.util.env import PROVIDER_ALIASES, get_provider_env, normalize_provider


class TestNormalizeProvider:
    """Tests for normalize_provider function."""

    def test_postgres_aliases(self):
        """Test all PostgreSQL aliases normalize to 'postgres'."""
        assert normalize_provider("postgresql") == "postgres"
        assert normalize_provider("postgres") == "postgres"
        assert normalize_provider("pg") == "postgres"

    def test_memgraph_alias(self):
        """Test Memgraph alias normalizes correctly."""
        assert normalize_provider("memgraph") == "memgraph"

    def test_case_insensitivity(self):
        """Test that normalization is case-insensitive."""
        assert normalize_provider("POSTGRESQL") == "postgres"
        assert normalize_provider("PostgreSQL") == "postgres"
        assert normalize_provider("PG") == "postgres"
        assert normalize_provider("Pg") == "postgres"
        assert normalize_provider("MEMGRAPH") == "memgraph"
        assert normalize_provider("Memgraph") == "memgraph"

    def test_whitespace_stripping(self):
        """Test that leading/trailing whitespace is stripped."""
        assert normalize_provider("  postgres  ") == "postgres"
        assert normalize_provider("\tpg\n") == "postgres"
        assert normalize_provider("  memgraph  ") == "memgraph"

    def test_unknown_provider_passes_through(self):
        """Test that unknown providers pass through lowercase."""
        assert normalize_provider("custom") == "custom"
        assert normalize_provider("CUSTOM") == "custom"
        assert normalize_provider("my-provider") == "my-provider"

    def test_empty_string(self):
        """Test empty string handling."""
        assert normalize_provider("") == ""
        assert normalize_provider("   ") == ""

    def test_alias_registry_completeness(self):
        """Verify PROVIDER_ALIASES contains expected mappings."""
        # PostgreSQL aliases
        assert "postgresql" in PROVIDER_ALIASES
        assert "postgres" in PROVIDER_ALIASES
        assert "pg" in PROVIDER_ALIASES
        # Memgraph alias
        assert "memgraph" in PROVIDER_ALIASES


class TestGetProviderEnv:
    """Tests for get_provider_env function."""

    def test_returns_default_when_env_not_set(self):
        """Test default value is returned when env var is not set."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_provider_env("UNSET_PROVIDER", "postgres", {"postgres", "memgraph"})
            assert result == "postgres"

    def test_returns_normalized_value_when_set(self):
        """Test env var value is normalized before returning."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "PostgreSQL"}):
            result = get_provider_env("TEST_PROVIDER", "memgraph", {"postgres", "memgraph"})
            assert result == "postgres"

    def test_accepts_valid_alias(self):
        """Test that valid aliases are accepted and normalized."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "PG"}):
            result = get_provider_env("TEST_PROVIDER", "memgraph", {"postgres", "memgraph"})
            assert result == "postgres"

    def test_raises_for_invalid_provider(self):
        """Test ValueError is raised for invalid provider values."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "invalid"}):
            with pytest.raises(ValueError) as exc_info:
                get_provider_env("TEST_PROVIDER", "postgres", {"postgres", "memgraph"})

            error_msg = str(exc_info.value)
            assert "TEST_PROVIDER" in error_msg
            assert "invalid" in error_msg
            assert "postgres" in error_msg
            assert "memgraph" in error_msg

    def test_error_message_includes_all_details(self):
        """Test error message format includes var name, value, and allowed list."""
        with patch.dict(os.environ, {"MY_PROVIDER": "bad-value"}):
            with pytest.raises(ValueError) as exc_info:
                get_provider_env("MY_PROVIDER", "postgres", {"postgres", "memgraph"})

            error_msg = str(exc_info.value)
            assert "Invalid provider for MY_PROVIDER" in error_msg
            assert "'bad-value'" in error_msg
            assert "Allowed values:" in error_msg

    def test_case_insensitive_validation(self):
        """Test that validation works with case variations."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "POSTGRES"}):
            result = get_provider_env("TEST_PROVIDER", "memgraph", {"postgres", "memgraph"})
            assert result == "postgres"

    def test_whitespace_handling(self):
        """Test that whitespace in env values is handled."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "  postgres  "}):
            result = get_provider_env("TEST_PROVIDER", "memgraph", {"postgres", "memgraph"})
            assert result == "postgres"

    def test_single_allowed_value(self):
        """Test with only one allowed value."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "postgres"}):
            result = get_provider_env("TEST_PROVIDER", "postgres", {"postgres"})
            assert result == "postgres"

    def test_multiple_allowed_values(self):
        """Test with multiple allowed values."""
        allowed = {"postgres", "memgraph", "mysql", "sqlite"}
        with patch.dict(os.environ, {"TEST_PROVIDER": "memgraph"}):
            result = get_provider_env("TEST_PROVIDER", "postgres", allowed)
            assert result == "memgraph"

    def test_empty_allowed_set_always_fails(self):
        """Test that empty allowed set fails for any value."""
        with patch.dict(os.environ, {"TEST_PROVIDER": "postgres"}):
            with pytest.raises(ValueError):
                get_provider_env("TEST_PROVIDER", "postgres", set())


class TestIntegration:
    """Integration tests simulating real usage patterns."""

    def test_dal_provider_env_vars(self):
        """Test typical DAL provider env var patterns."""
        allowed = {"postgres", "memgraph"}

        env_vars = {
            "CACHE_STORE_PROVIDER": "PostgreSQL",
            "GRAPH_STORE_PROVIDER": "Memgraph",
            "SCHEMA_STORE_PROVIDER": "pg",
        }

        with patch.dict(os.environ, env_vars):
            assert get_provider_env("CACHE_STORE_PROVIDER", "postgres", allowed) == "postgres"
            assert get_provider_env("GRAPH_STORE_PROVIDER", "postgres", allowed) == "memgraph"
            assert get_provider_env("SCHEMA_STORE_PROVIDER", "postgres", allowed) == "postgres"

    def test_defaults_match_current_setup(self):
        """Test that defaults reflect current Postgres+Memgraph setup."""
        allowed = {"postgres", "memgraph"}

        with patch.dict(os.environ, {}, clear=True):
            # Most stores default to postgres
            assert get_provider_env("CACHE_STORE_PROVIDER", "postgres", allowed) == "postgres"
            assert get_provider_env("EXAMPLE_STORE_PROVIDER", "postgres", allowed) == "postgres"
            assert get_provider_env("SCHEMA_STORE_PROVIDER", "postgres", allowed) == "postgres"

            # Graph store defaults to memgraph
            assert get_provider_env("GRAPH_STORE_PROVIDER", "memgraph", allowed) == "memgraph"

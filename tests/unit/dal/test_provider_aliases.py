"""Unit tests for provider alias normalization and validation."""

import os
from unittest.mock import patch

import pytest

from dal.util.env import PROVIDER_ALIASES, get_provider_env, normalize_provider

QUERY_TARGET_ALLOWED = {
    "postgres",
    "sqlite",
    "mysql",
    "snowflake",
    "redshift",
    "bigquery",
    "athena",
    "databricks",
    "cockroachdb",
    "duckdb",
    "clickhouse",
}

GRAPH_STORE_ALLOWED = {"memgraph"}


class TestProviderAliases:
    """Validate provider alias normalization across the registry."""

    def test_all_aliases_resolve_to_canonical(self) -> None:
        """Every alias should resolve to a known canonical provider."""
        allowed = QUERY_TARGET_ALLOWED | GRAPH_STORE_ALLOWED
        for alias, canonical in PROVIDER_ALIASES.items():
            assert normalize_provider(alias) == canonical
            assert canonical in allowed, f"{alias} resolves to unknown provider {canonical}"

    @pytest.mark.parametrize("canonical", sorted(QUERY_TARGET_ALLOWED))
    def test_canonical_query_target_providers_are_accepted(self, canonical: str) -> None:
        """Each canonical query-target provider should be accepted by validation."""
        with patch.dict(os.environ, {"QUERY_TARGET_BACKEND": canonical}):
            assert (
                get_provider_env("QUERY_TARGET_BACKEND", "postgres", QUERY_TARGET_ALLOWED)
                == canonical
            )

    def test_memgraph_alias_is_accepted_for_graph_store(self) -> None:
        """Memgraph aliases should validate against graph store providers."""
        with patch.dict(os.environ, {"GRAPH_STORE_PROVIDER": "memgraph"}):
            assert (
                get_provider_env("GRAPH_STORE_PROVIDER", "memgraph", GRAPH_STORE_ALLOWED)
                == "memgraph"
            )

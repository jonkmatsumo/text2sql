"""Unit tests for provider alias normalization and resolution."""

import pytest

from dal.util.env import PROVIDER_ALIASES, normalize_provider


class TestProviderAliases:
    """Validate that all provider aliases resolve correctly."""

    @pytest.mark.parametrize(
        "alias,expected",
        [
            # PostgreSQL aliases
            ("postgresql", "postgres"),
            ("postgres", "postgres"),
            ("pg", "postgres"),
            ("POSTGRES", "postgres"),
            ("  PostgreSQL  ", "postgres"),
            # SQLite aliases
            ("sqlite", "sqlite"),
            ("sqlite3", "sqlite"),
            ("SQLITE3", "sqlite"),
            # MySQL aliases
            ("mysql", "mysql"),
            ("mariadb", "mysql"),
            ("MariaDB", "mysql"),
            # Snowflake aliases
            ("snowflake", "snowflake"),
            ("sf", "snowflake"),
            ("SF", "snowflake"),
            # BigQuery aliases
            ("bigquery", "bigquery"),
            ("bq", "bigquery"),
            ("BQ", "bigquery"),
            # Redshift
            ("redshift", "redshift"),
            ("REDSHIFT", "redshift"),
            # Athena
            ("athena", "athena"),
            # Databricks aliases
            ("databricks", "databricks"),
            ("databricks-sql", "databricks"),
            ("DATABRICKS-SQL", "databricks"),
            # YugabyteDB aliases -> postgres
            ("yugabyte", "postgres"),
            ("ysql", "postgres"),
            ("YSQL", "postgres"),
            # CockroachDB aliases
            ("cockroachdb", "cockroachdb"),
            ("cockroach", "cockroachdb"),
            ("crdb", "cockroachdb"),
            ("CRDB", "cockroachdb"),
            # DuckDB
            ("duckdb", "duckdb"),
            ("DUCKDB", "duckdb"),
            # ClickHouse aliases
            ("clickhouse", "clickhouse"),
            ("ch", "clickhouse"),
            ("CH", "clickhouse"),
            # Memgraph
            ("memgraph", "memgraph"),
        ],
    )
    def test_normalize_provider_aliases(self, alias: str, expected: str):
        """Ensure alias normalization returns the canonical provider ID."""
        assert normalize_provider(alias) == expected

    def test_normalize_provider_unknown_passthrough(self):
        """Unknown providers pass through unchanged (lowercased)."""
        assert normalize_provider("custom-provider") == "custom-provider"
        assert normalize_provider("CUSTOM") == "custom"

    def test_normalize_provider_strips_whitespace(self):
        """Whitespace is stripped before normalization."""
        assert normalize_provider("  postgres  ") == "postgres"
        assert normalize_provider("\tsnowflake\n") == "snowflake"

    def test_all_aliases_have_canonical_targets(self):
        """All aliases in PROVIDER_ALIASES map to known canonical IDs."""
        canonical_ids = {
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
            "memgraph",
        }
        for alias, target in PROVIDER_ALIASES.items():
            assert target in canonical_ids, f"Alias '{alias}' maps to unknown target '{target}'"

    def test_yugabyte_aliases_resolve_to_postgres(self):
        """The YugabyteDB aliases specifically resolve to postgres."""
        assert normalize_provider("yugabyte") == "postgres"
        assert normalize_provider("ysql") == "postgres"

    def test_cockroach_aliases_resolve_to_cockroachdb(self):
        """The CockroachDB aliases resolve to cockroachdb."""
        assert normalize_provider("cockroach") == "cockroachdb"
        assert normalize_provider("crdb") == "cockroachdb"
        assert normalize_provider("cockroachdb") == "cockroachdb"

    def test_mariadb_alias_resolves_to_mysql(self):
        """The MariaDB alias resolves to the mysql provider."""
        assert normalize_provider("mariadb") == "mysql"

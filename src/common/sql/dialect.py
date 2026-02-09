"""Shared utilities for SQL dialect handling."""

from typing import Optional


def normalize_sqlglot_dialect(dialect: Optional[str]) -> str:
    """Normalize a dialect name for use with sqlglot.

    Args:
        dialect: The dialect name to normalize (e.g., 'PostgreSQL', 'DuckDB').

    Returns:
        A normalized lowercase string compatible with sqlglot.
    """
    if not dialect:
        return "postgres"

    d = dialect.lower().strip()

    # Map common aliases to sqlglot names
    mapping = {
        "postgresql": "postgres",
        "pg": "postgres",
        "duck": "duckdb",
        "sqlite": "sqlite",
        "mysql": "mysql",
        "oracle": "oracle",
        "bigquery": "bigquery",
        "snowflake": "snowflake",
        "redshift": "redshift",
        "clickhouse": "clickhouse",
        "databricks": "databricks",
        "trino": "trino",
        "presto": "presto",
        "spark": "spark",
    }

    return mapping.get(d, d)

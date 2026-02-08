"""Unit tests for SQL dialect consistency."""

import sqlglot

from common.sql.dialect import normalize_sqlglot_dialect


def test_normalize_sqlglot_dialect():
    """Verify normalization of various dialect names."""
    assert normalize_sqlglot_dialect("PostgreSQL") == "postgres"
    assert normalize_sqlglot_dialect("PG") == "postgres"
    assert normalize_sqlglot_dialect("DuckDB") == "duckdb"
    assert normalize_sqlglot_dialect("Duck") == "duckdb"
    assert normalize_sqlglot_dialect(None) == "postgres"
    assert normalize_sqlglot_dialect("unknown") == "unknown"


def test_dialect_parsing_consistency():
    """Verify that agent and MCP normalized dialects result in same parsing behavior."""
    sql = "SELECT * FROM my_table LIMIT 10"

    # Test with aliases that should normalize to same thing
    d1 = normalize_sqlglot_dialect("PostgreSQL")
    d2 = normalize_sqlglot_dialect("PG")

    assert d1 == d2 == "postgres"

    ast1 = sqlglot.parse_one(sql, read=d1)
    ast2 = sqlglot.parse_one(sql, read=d2)

    assert str(ast1) == str(ast2)


def test_duckdb_parsing():
    """Verify DuckDB specific parsing."""
    sql = "SELECT * FROM 'data.csv'"
    dialect = normalize_sqlglot_dialect("DuckDB")

    # This should parse in DuckDB but might fail in Postgres
    ast = sqlglot.parse_one(sql, read=dialect)
    assert ast.key == "select"

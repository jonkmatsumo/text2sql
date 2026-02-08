"""Integration tests for AST validation conformance across dialects."""

import pytest

from mcp_server.tools.execute_sql_query import _validate_sql_ast

# Dialects to test
DIALECTS = [
    "postgres",
    "bigquery",
    "snowflake",
    "redshift",
    "mysql",
    "sqlite",
    "duckdb",
]

# Common allowed queries (valid across all tested dialects)
COMMON_ALLOWED_QUERIES = [
    "SELECT * FROM users",
    "SELECT id, name FROM users WHERE id > 100",
    "SELECT count(*) FROM orders GROUP BY user_id",
    (
        "WITH regional_sales AS "
        "(SELECT region, SUM(amount) AS total_sales FROM orders GROUP BY region) "
        "SELECT region, total_sales FROM regional_sales WHERE total_sales > "
        "(SELECT SUM(total_sales)/10 FROM regional_sales)"
    ),
    "SELECT * FROM users UNION ALL SELECT * FROM deleted_users",
    "SELECT 1",
    "SELECT 'drop table users'",
    "-- select all users\nSELECT * FROM users",
    "/* complex comment */ SELECT * FROM users",
]

# Queries that are valid in most dialects but strictly require DISTINCT/ALL in BigQuery
IMPLICIT_SET_OPS = [
    "SELECT * FROM users UNION SELECT * FROM deleted_users",
    "SELECT * FROM users INTERSECT SELECT * FROM priority_users",
    "SELECT * FROM users EXCEPT SELECT * FROM blocked_users",
]

# Explicit set operations for BigQuery
BIGQUERY_SET_OPS = [
    "SELECT * FROM users UNION DISTINCT SELECT * FROM deleted_users",
    "SELECT * FROM users INTERSECT DISTINCT SELECT * FROM priority_users",
    "SELECT * FROM users EXCEPT DISTINCT SELECT * FROM blocked_users",
]

# Forbidden queries (should return error string)
FORBIDDEN_QUERIES = [
    "INSERT INTO users (name) VALUES ('Alice')",
    "UPDATE users SET name = 'Bob' WHERE id = 1",
    "DELETE FROM users WHERE id = 1",
    "DROP TABLE users",
    "ALTER TABLE users ADD COLUMN age INT",
    "TRUNCATE TABLE users",
    "GRANT SELECT ON users TO 'bob'",
    "REVOKE SELECT ON users FROM 'bob'",
    "CREATE TABLE new_users AS SELECT * FROM users",
    (
        "MERGE INTO target USING source ON target.id = source.id "
        "WHEN MATCHED THEN UPDATE SET target.col = source.col"
    ),
    "CALL some_procedure()",
    "EXPLAIN SELECT * FROM users",
    "BEGIN; SELECT 1; COMMIT;",
    "SELECT 1; DROP TABLE users",
    "SELECT 1; SELECT 2",
    "foo bar",
    "",
]


class TestASTConformance:
    """Parameterized conformance tests for SQL validation."""

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("query", COMMON_ALLOWED_QUERIES)
    def test_common_allowed_queries(self, dialect, query):
        """Verify common queries pass validation across all dialects."""
        error = _validate_sql_ast(query, dialect)
        assert error is None, f"Dialect {dialect} failed valid query: {query}. Error: {error}"

    @pytest.mark.parametrize("dialect", DIALECTS)
    def test_set_operations(self, dialect):
        """Verify set operations (UNION, INTERSECT, EXCEPT) with dialect nuances."""
        if dialect == "bigquery":
            # BigQuery requires explicit DISTINCT or ALL
            queries = BIGQUERY_SET_OPS
        else:
            # Other dialects usually allow implicit DISTINCT (standard SQL behavior)
            # Note: Explicit DISTINCT is also valid in postgres, but we test the implicit case here
            queries = IMPLICIT_SET_OPS

        for query in queries:
            error = _validate_sql_ast(query, dialect)
            assert error is None, f"Dialect {dialect} failed valid set op: {query}. Error: {error}"

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("query", FORBIDDEN_QUERIES)
    def test_forbidden_queries(self, dialect, query):
        """Verify forbidden queries are rejected across all dialects."""
        error = _validate_sql_ast(query, dialect)
        assert error is not None, f"Dialect {dialect} allowed forbidden query: {query}"

        # Check specific error messages for clarity
        if ";" in query and not query.strip().startswith("BEGIN"):
            assert "Multi-statement queries are forbidden" in error or "Syntax Error" in error
        elif "DROP" in query.upper() and "SELECT" not in query.upper():
            assert "Forbidden statement type" in error or "Syntax Error" in error

    def test_dialect_mapping_defaults(self):
        """Test fallback to postgres for unknown dialects."""
        assert _validate_sql_ast("SELECT 1", "unknown_provider") is None
        assert _validate_sql_ast("DROP TABLE users", "unknown_provider") is not None

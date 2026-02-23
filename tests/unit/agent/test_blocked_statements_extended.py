"""Extended coverage for high-risk SQL statement blocking."""

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer
from mcp_server.tools.execute_sql_query import _validate_sql_ast


@pytest.fixture(autouse=True)
def _mock_allowed_tables() -> None:
    """Use a static allowlist to avoid DB introspection during tests."""
    PolicyEnforcer.set_allowed_tables({"users", "customer"})
    yield
    PolicyEnforcer.set_allowed_tables(None)


BLOCKED_STATEMENT_CASES = [
    ("COPY", "COPY (SELECT 1) TO STDOUT"),
    ("COPY", "COPY (SELECT 1) TO PROGRAM 'cat'"),
    ("DO", "DO $$ BEGIN RAISE NOTICE 'x'; END $$"),
    ("PREPARE", "PREPARE stmt AS SELECT 1"),
    ("EXECUTE", "EXECUTE stmt"),
    ("DEALLOCATE", "DEALLOCATE stmt"),
    ("CALL", "CALL my_proc()"),
    (
        "CREATE FUNCTION",
        "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
    ),
    (
        "CREATE PROCEDURE",
        "CREATE PROCEDURE p() LANGUAGE SQL AS $$ SELECT 1 $$",
    ),
    ("CREATE EXTENSION", "CREATE EXTENSION IF NOT EXISTS dblink"),
]


@pytest.mark.parametrize(("statement_name", "sql"), BLOCKED_STATEMENT_CASES)
def test_policy_enforcer_blocks_high_risk_statements(statement_name: str, sql: str) -> None:
    """Agent-side policy should reject high-risk statement-level bypass vectors."""
    with pytest.raises(ValueError, match="restricted") as exc_info:
        PolicyEnforcer.validate_sql(sql)

    assert statement_name.lower() in str(exc_info.value).lower()


@pytest.mark.parametrize(("statement_name", "sql"), BLOCKED_STATEMENT_CASES)
def test_mcp_ast_validation_blocks_high_risk_statements(statement_name: str, sql: str) -> None:
    """MCP-side AST validation should reject high-risk statement-level bypass vectors."""
    error = _validate_sql_ast(sql, "postgres")

    assert isinstance(error, str)
    assert "Forbidden statement" in error
    assert statement_name.upper() in error.upper()

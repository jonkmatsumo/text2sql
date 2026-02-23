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

SIDE_EFFECT_STATEMENT_CASES = [
    ("SET", "SET ROLE some_role"),
    ("SET", "SET search_path TO public"),
    ("RESET", "RESET ALL"),
    ("ALTER SYSTEM", "ALTER SYSTEM SET work_mem = '4MB'"),
    ("ALTER ROLE", "ALTER ROLE app_user SET search_path TO public"),
    ("CREATE ROLE", "CREATE ROLE app_user"),
    ("GRANT", "GRANT SELECT ON users TO app_user"),
    ("REVOKE", "REVOKE SELECT ON users FROM app_user"),
    ("VACUUM", "VACUUM users"),
    ("ANALYZE", "ANALYZE users"),
    ("LISTEN", "LISTEN safety_channel"),
    ("NOTIFY", "NOTIFY safety_channel"),
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


@pytest.mark.parametrize(("statement_name", "sql"), SIDE_EFFECT_STATEMENT_CASES)
def test_policy_enforcer_blocks_session_and_privilege_side_effects(
    statement_name: str, sql: str
) -> None:
    """Agent-side policy should reject session and privilege side-effect statements."""
    with pytest.raises(ValueError, match="restricted") as exc_info:
        PolicyEnforcer.validate_sql(sql)

    assert statement_name.lower() in str(exc_info.value).lower()


@pytest.mark.parametrize(("statement_name", "sql"), SIDE_EFFECT_STATEMENT_CASES)
def test_mcp_ast_validation_blocks_session_and_privilege_side_effects(
    statement_name: str, sql: str
) -> None:
    """MCP-side validation should reject session and privilege side-effect statements."""
    error = _validate_sql_ast(sql, "postgres")

    assert isinstance(error, str)
    assert "Forbidden statement" in error
    assert statement_name.upper() in error.upper()

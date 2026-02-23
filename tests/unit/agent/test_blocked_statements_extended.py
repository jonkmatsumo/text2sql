"""Extended coverage for high-risk SQL statement blocking.

Tests both enforcement surfaces for statement-level bypass vectors:
  - Agent ``PolicyEnforcer.validate_sql()``
  - MCP ``_validate_sql_ast()`` / ``_validate_sql_ast_failure()``
"""

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer, PolicyValidationError
from mcp_server.tools.execute_sql_query import _validate_sql_ast, _validate_sql_ast_failure


@pytest.fixture(autouse=True)
def _mock_allowed_tables() -> None:
    """Use a static allowlist to avoid DB introspection during tests."""
    PolicyEnforcer.set_allowed_tables({"users", "customer", "t"})
    yield
    PolicyEnforcer.set_allowed_tables(None)


# ---------------------------------------------------------------------------
# Phase 2: High-risk statement vectors — COPY, DO, PREPARE/EXECUTE, CALL, EXTENSION
# ---------------------------------------------------------------------------

BLOCKED_STATEMENT_CASES = [
    # COPY: file-path variants
    ("COPY", "COPY t TO '/tmp/exfil.csv'"),
    ("COPY", "COPY t FROM '/tmp/inject.csv'"),
    # COPY: query-wrapper → STDOUT
    ("COPY", "COPY (SELECT 1) TO STDOUT"),
    # COPY: PROGRAM (OS command execution)
    ("COPY", "COPY t TO PROGRAM 'cat > /tmp/pwned'"),
    ("COPY", "COPY t FROM PROGRAM 'cat /etc/passwd'"),
    # DO: anonymous PL/pgSQL block
    ("DO", "DO $$ BEGIN RAISE NOTICE 'x'; END $$"),
    ("DO", "DO LANGUAGE plpgsql $$ BEGIN NULL; END $$"),
    # PREPARE: various body types
    ("PREPARE", "PREPARE stmt AS SELECT 1"),
    ("PREPARE", "PREPARE q AS DELETE FROM t"),
    # EXECUTE: standalone
    ("EXECUTE", "EXECUTE stmt"),
    # DEALLOCATE
    ("DEALLOCATE", "DEALLOCATE stmt"),
    ("DEALLOCATE", "DEALLOCATE ALL"),
    # CALL
    ("CALL", "CALL my_proc()"),
    ("CALL", "CALL do_write(42, 'x')"),
    # CREATE FUNCTION / PROCEDURE
    (
        "CREATE FUNCTION",
        "CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
    ),
    (
        "CREATE PROCEDURE",
        "CREATE PROCEDURE p() LANGUAGE SQL AS $$ SELECT 1 $$",
    ),
    # CREATE EXTENSION
    ("CREATE EXTENSION", "CREATE EXTENSION IF NOT EXISTS dblink"),
    ("CREATE EXTENSION", "CREATE EXTENSION pg_cron"),
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


@pytest.mark.parametrize(("statement_name", "sql"), BLOCKED_STATEMENT_CASES)
def test_high_risk_statement_rejection_is_sanitized(statement_name: str, sql: str) -> None:
    """Rejection messages must not echo back the raw SQL (sanitization requirement)."""
    with pytest.raises(PolicyValidationError) as a_exc:
        PolicyEnforcer.validate_sql(sql)

    # The error message should not contain the literal SQL being validated.
    assert sql.lower() not in str(a_exc.value).lower()

    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None
    assert sql.lower() not in mcp_failure.message.lower()

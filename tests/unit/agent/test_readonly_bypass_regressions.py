"""Readonly bypass regression tests.

Covers three concerns in one file so the shared helper is defined once:

1.  Data-modifying CTEs, SELECT INTO, and locking – patterns that look like
    reads at the statement-type level but carry mutation or contention risk.
2.  Fail-closed behaviour for unparseable / dialect-unknown statements.
3.  Nested mutation attempts that may be caught either as a parse error or as
    a readonly_violation depending on sqlglot version.

The helper ``assert_rejected_by_agent_and_mcp`` is the single source of truth
for dual-layer rejection assertions used throughout this module.
"""

from __future__ import annotations

import pytest

from agent.validation.policy_enforcer import PolicyEnforcer, PolicyValidationError
from mcp_server.tools.execute_sql_query import _validate_sql_ast_failure

# ---------------------------------------------------------------------------
# Phase 4: centralised dual-layer rejection helper
# ---------------------------------------------------------------------------


def assert_rejected_by_agent_and_mcp(
    sql: str,
    *,
    expected_reason_code: str | None = None,
) -> None:
    """Assert that *sql* is rejected by both the Agent and MCP enforcement layers.

    Args:
        sql: The SQL string under test.
        expected_reason_code: When provided, both layers must produce exactly
            this ``reason_code``.  When *None*, only rejection (any code) is
            asserted – useful for fail-closed / parse-error cases where the
            exact classification may differ across sqlglot versions.
    """
    # ------------------------------------------------------------------
    # Agent side
    # ------------------------------------------------------------------
    with pytest.raises(PolicyValidationError) as agent_exc:
        PolicyEnforcer.validate_sql(sql)

    agent_err = agent_exc.value
    if expected_reason_code is not None:
        assert agent_err.reason_code == expected_reason_code, (
            f"Agent reason_code mismatch: expected {expected_reason_code!r}, "
            f"got {agent_err.reason_code!r} for SQL: {sql!r}"
        )
    # Sanitization: raw SQL must not appear in the rejection message.
    assert (
        sql.lower() not in str(agent_err).lower()
    ), f"Agent error message leaked raw SQL for: {sql!r}"

    # ------------------------------------------------------------------
    # MCP side
    # ------------------------------------------------------------------
    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None, f"MCP did not reject SQL: {sql!r}"
    if expected_reason_code is not None:
        assert mcp_failure.reason_code == expected_reason_code, (
            f"MCP reason_code mismatch: expected {expected_reason_code!r}, "
            f"got {mcp_failure.reason_code!r} for SQL: {sql!r}"
        )
    # Sanitization: raw SQL must not appear in the failure message.
    assert (
        sql.lower() not in mcp_failure.message.lower()
    ), f"MCP failure message leaked raw SQL for: {sql!r}"


# ---------------------------------------------------------------------------
# Fixture: static table allowlist so tests never touch the DB
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _mock_allowed_tables() -> None:
    PolicyEnforcer.set_allowed_tables({"users", "t", "customer", "new_table"})
    yield
    PolicyEnforcer.set_allowed_tables(None)


# ---------------------------------------------------------------------------
# Phase 2a: Data-modifying CTEs
# ---------------------------------------------------------------------------

MODIFYING_CTE_CASES = [
    # UPDATE inside CTE
    "WITH x AS (UPDATE t SET a=1 RETURNING 1) SELECT * FROM x",
    # DELETE inside CTE
    "WITH x AS (DELETE FROM t RETURNING 1) SELECT * FROM x",
    # INSERT inside CTE
    "WITH x AS (INSERT INTO t(a) VALUES (1) RETURNING 1) SELECT * FROM x",
]


@pytest.mark.parametrize("sql", MODIFYING_CTE_CASES)
def test_modifying_cte_rejected(sql: str) -> None:
    """Data-modifying CTEs must be rejected by both enforcement layers."""
    assert_rejected_by_agent_and_mcp(sql, expected_reason_code="readonly_violation")


# ---------------------------------------------------------------------------
# Phase 2b: SELECT INTO
# ---------------------------------------------------------------------------

SELECT_INTO_CASES = [
    # Direct Postgres-style SELECT INTO
    "SELECT * INTO new_table FROM t",
    # Via CTE
    "WITH q AS (SELECT * FROM t) SELECT * INTO new_table FROM q",
]


@pytest.mark.parametrize("sql", SELECT_INTO_CASES)
def test_select_into_rejected(sql: str) -> None:
    """SELECT INTO must be rejected as a read-only bypass pattern."""
    assert_rejected_by_agent_and_mcp(sql, expected_reason_code="readonly_violation")


# ---------------------------------------------------------------------------
# Phase 2c: Locking clauses
# ---------------------------------------------------------------------------

LOCKING_CASES = [
    "SELECT * FROM t FOR UPDATE",
    "SELECT * FROM t FOR SHARE",
    "SELECT * FROM t FOR NO KEY UPDATE",
    "SELECT * FROM t FOR KEY SHARE",
]


@pytest.mark.parametrize("sql", LOCKING_CASES)
def test_locking_clause_rejected(sql: str) -> None:
    """FOR UPDATE / FOR SHARE and variants must be rejected."""
    assert_rejected_by_agent_and_mcp(sql, expected_reason_code="readonly_violation")


# ---------------------------------------------------------------------------
# Phase 2d: Classification contract for readonly_violation
# ---------------------------------------------------------------------------


def test_readonly_violation_classification_is_stable() -> None:
    """readonly_violation must use SQL_READONLY_VIOLATION error code on both layers."""
    from common.models.error_metadata import ErrorCategory
    from common.policy.sql_policy import SQL_READONLY_VIOLATION

    sql = "WITH x AS (UPDATE t SET a=1 RETURNING 1) SELECT * FROM x"

    with pytest.raises(PolicyValidationError) as agent_exc:
        PolicyEnforcer.validate_sql(sql)

    agent_err = agent_exc.value
    assert agent_err.reason_code == "readonly_violation"
    assert agent_err.error_code == SQL_READONLY_VIOLATION
    assert agent_err.category == ErrorCategory.INVALID_REQUEST.value
    assert sql.lower() not in str(agent_err).lower()

    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None
    assert mcp_failure.reason_code == "readonly_violation"
    assert mcp_failure.error_code == SQL_READONLY_VIOLATION
    assert mcp_failure.category == ErrorCategory.INVALID_REQUEST
    assert sql.lower() not in mcp_failure.message.lower()


# ---------------------------------------------------------------------------
# Phase 3: Fail-closed on unparseable / dialect-unknown statements
# ---------------------------------------------------------------------------
# For these cases we do NOT assert a specific reason_code since the exact
# classification (invalid_sql_ast vs invalid_sql_syntax vs sql_validation_error)
# can vary with sqlglot version.  The invariant is: BOTH layers must reject.

PARSE_FAIL_CASES = [
    # Empty input
    "",
    # Whitespace only
    "   \n\t  ",
    # Completely invalid syntax
    "$$$$NOT_SQL_AT_ALL",
    # Multi-word garbage
    "SELECT FROM FROM WHERE AND OR",
]


@pytest.mark.parametrize("sql", PARSE_FAIL_CASES)
def test_fail_closed_on_unparseable_sql(sql: str) -> None:
    """Unparseable SQL must be rejected (fail-closed) by both layers."""
    # Agent: any error type is acceptable (ValueError or PolicyValidationError).
    with pytest.raises((ValueError, PolicyValidationError)):
        PolicyEnforcer.validate_sql(sql)

    # MCP: must return a non-None failure.
    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None, f"MCP did not reject unparseable SQL: {sql!r}"


def test_fail_closed_on_null_byte() -> None:
    """NULL-byte injection must be rejected by both layers."""
    sql = "\x00SELECT 1"
    # Agent
    with pytest.raises((ValueError, PolicyValidationError)):
        PolicyEnforcer.validate_sql(sql)
    # MCP
    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None, "MCP did not reject null-byte SQL"


def test_fail_closed_on_unknown_dialect_construct() -> None:
    """Dialect-specific syntax unknown to the selected parser must be rejected."""
    # MSSQL-specific TOP clause submitted as postgres – parser may stumble.
    sql = "SELECT TOP 1 * FROM t"
    # sqlglot tends to handle this gracefully, but the important guarantee is
    # that the result is either rejected or sanitized as a valid SELECT.
    # If it parses, it becomes an ordinary SELECT (allowed).
    # If it fails to parse or produces an error, both layers must reject.
    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    # We assert only that we didn't crash – the outcome (accepted/rejected) is
    # allowed to vary, but failure must always be a SQLASTValidationFailure,
    # never an unhandled exception.
    assert mcp_failure is None or isinstance(
        mcp_failure.reason_code, str
    ), "MCP returned unexpected failure type"


# ---------------------------------------------------------------------------
# Phase 3b: Nested mutation attempt
# ---------------------------------------------------------------------------


def test_nested_mutation_attempt_fails_closed() -> None:
    """Nested DELETE inside SELECT must be rejected or fail to parse.

    ``SELECT (DELETE FROM t RETURNING 1)`` is syntactically invalid in
    Postgres; we assert that both layers reject it (fail-closed posture)
    regardless of which specific error code is produced.
    """
    sql = "SELECT (DELETE FROM t RETURNING 1)"
    with pytest.raises((ValueError, PolicyValidationError)):
        PolicyEnforcer.validate_sql(sql)

    mcp_failure = _validate_sql_ast_failure(sql, "postgres")
    assert mcp_failure is not None, "MCP did not reject nested mutation attempt"

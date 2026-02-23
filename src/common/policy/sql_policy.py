"""Centralized SQL policy configuration.

Defines SQL statement, function, and sensitive-column policies shared by
Agent and MCP validation layers.
"""

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from sqlglot import exp

from common.config.env import get_env_str
from common.errors.error_codes import ErrorCode
from common.models.error_metadata import ErrorCategory

# Statement types allowed for execution
# Using strings for easy comparison with AST node keys or type names
ALLOWED_STATEMENT_TYPES: Set[str] = {
    "select",
    "union",
    "intersect",
    "except",
    "with",
}

# Dangerous functions that should be blocked in SQL queries
BLOCKED_FUNCTIONS: Set[str] = {
    # System disruption / DoS / Locking
    "pg_sleep",
    "sleep",
    "usleep",
    "sys_sleep",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "pg_advisory_lock_shared",
    "pg_advisory_xact_lock_shared",
    "pg_try_advisory_lock",
    "pg_try_advisory_xact_lock",
    "pg_try_advisory_lock_shared",
    "pg_try_advisory_xact_lock_shared",
    "pg_cancel_backend",
    "pg_terminate_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_log_backend_memory_contexts",
    # Remote execution / bypass
    "dblink",
    "dblink_exec",
    # Server filesystem access
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "pg_ls_tmpdir",
    "pg_ls_archive_status",
    "pg_ls_waldir",
    "pg_ls_logicalsnapdir",
    "pg_ls_logicalmapdir",
    "pg_ls_replslotdir",
    "lo_import",
    "lo_export",
    # Arbitrary subquery execution wrappers
    "query_to_xml",
    "query_to_json",
    # Config / Information leakage
    "current_setting",
    "set_config",
    "current_user",
    "session_user",
    "version",
}

# Tables that are always blocked unless explicitly overridden via SQL_BLOCKED_TABLES.
DEFAULT_BLOCKED_TABLES: Set[str] = {
    "payroll",
    "credentials",
    "audit_logs",
    "user_secrets",
    "password_history",
    "api_keys",
}

# Schemas blocked by default unless explicitly overridden via SQL_BLOCKED_SCHEMAS.
DEFAULT_BLOCKED_SCHEMAS: Set[str] = {
    "information_schema",
    "pg_catalog",
}

# Prefixes indicating internal/system tables.
BLOCKED_TABLE_PREFIXES: tuple[str, ...] = ("pg_",)

# Sensitive column-name markers used for safety guardrails.
# Matching is case-insensitive and substring-based.
SENSITIVE_COLUMN_NAME_PATTERNS: Set[str] = {
    "password",
    "token",
    "secret",
    "credential",
    "ssn",
    "api_key",
    "apikey",
}

_COMMAND_BLOCKLIST = frozenset(
    {
        "DO",
        "PREPARE",
        "EXECUTE",
        "DEALLOCATE",
        "CALL",
        "SET",
        "RESET",
        "VACUUM",
    }
)
_ALIAS_BLOCKLIST = frozenset({"DEALLOCATE", "RESET", "LISTEN", "NOTIFY"})

# Stable error code constants for cross-layer classification parity.
SQL_FORBIDDEN_FUNCTION_CODE = "SQL_FORBIDDEN_FUNCTION"
SQL_FORBIDDEN_STATEMENT_CODE = "SQL_FORBIDDEN_STATEMENT"
SQL_READONLY_VIOLATION = "SQL_READONLY_VIOLATION"


@dataclass(frozen=True)
class SQLPolicyViolation:
    """Structured SQL policy rejection produced by shared AST checks."""

    reason_code: str
    category: ErrorCategory = ErrorCategory.INVALID_REQUEST
    error_code: str = ErrorCode.VALIDATION_ERROR.value
    statement: str | None = None
    function: str | None = None


def is_sensitive_column_name(column_name: str) -> bool:
    """Return True when a column name matches a sensitive marker."""
    if not isinstance(column_name, str):
        return False
    normalized = column_name.strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in SENSITIVE_COLUMN_NAME_PATTERNS)


def extract_function_names(node: exp.Func) -> tuple[str, ...]:
    """Return deterministic candidate function names for blocklist checks."""
    names: set[str] = set()

    sql_name = node.sql_name()
    if sql_name:
        names.add(str(sql_name).lower())

    if isinstance(node, exp.Anonymous) and node.this:
        names.add(str(node.this).lower())

    if hasattr(node, "name") and node.name:
        names.add(str(node.name).lower())

    return tuple(sorted(names))


def _normalize_statement_keyword(raw: object) -> str:
    if raw is None:
        return ""
    if isinstance(raw, exp.Expression):
        candidate = raw.sql(dialect="postgres")
    else:
        candidate = str(raw)
    return candidate.strip().strip(";").upper()


def _command_keyword(statement: exp.Command) -> str:
    return _normalize_statement_keyword(statement.this)


def _command_remainder(statement: exp.Command) -> str:
    return _normalize_statement_keyword(statement.expression)


def _alias_keyword(statement: exp.Alias) -> str:
    if isinstance(statement.this, exp.Column):
        return _normalize_statement_keyword(statement.this.name)
    if isinstance(statement.this, exp.Identifier):
        return _normalize_statement_keyword(statement.this.this)
    return ""


def classify_blocked_statement(
    statement: exp.Expression,
) -> str | None:  # noqa: C901 (deliberate exhaustive match)
    """Classify explicit high-risk statements that must always be rejected.

    Returns a canonical statement label string (e.g. ``"COPY"``, ``"DO"``,
    ``"CREATE EXTENSION"``) or *None* when the statement is not inherently
    forbidden (further checks such as the SELECT-only allowlist still apply).

    Detection order: specific typed AST nodes first (cheapest and most
    reliable), then ``exp.Command`` fallback for statements that sqlglot
    cannot represent natively in the target dialect.
    """
    # ------------------------------------------------------------------
    # Phase 1: Strongly-typed AST node checks
    # ------------------------------------------------------------------

    # COPY … TO/FROM [FILE|PROGRAM|STDIN|STDOUT] — encompasses all variants
    # including the dangerous COPY … TO PROGRAM and COPY … FROM PROGRAM.
    if isinstance(statement, exp.Copy):
        return "COPY"

    if isinstance(statement, exp.Set):
        return "SET"

    if isinstance(statement, exp.Grant):
        return "GRANT"

    if isinstance(statement, exp.Revoke):
        return "REVOKE"

    if isinstance(statement, exp.Analyze):
        return "ANALYZE"

    if isinstance(statement, exp.Create):
        kind = _normalize_statement_keyword(statement.args.get("kind"))
        if kind == "EXTENSION":
            return "CREATE EXTENSION"
        if kind in {"FUNCTION", "PROCEDURE"}:
            return f"CREATE {kind}"
        if kind in {"ROLE", "USER"}:
            return f"CREATE {kind}"

    # ------------------------------------------------------------------
    # Phase 2: exp.Command fallback — catches statements sqlglot cannot
    # represent as first-class nodes in the postgres dialect (e.g. DO,
    # PREPARE, EXECUTE, DEALLOCATE, CALL, ALTER SYSTEM, RESET, LISTEN …)
    # ------------------------------------------------------------------
    if isinstance(statement, exp.Command):
        keyword = _command_keyword(statement)
        if keyword in _COMMAND_BLOCKLIST:
            return keyword
        if keyword == "ALTER":
            remainder = _command_remainder(statement)
            if remainder.startswith("SYSTEM"):
                return "ALTER SYSTEM"
            if remainder.startswith("ROLE"):
                return "ALTER ROLE"
            if remainder.startswith("USER"):
                return "ALTER USER"
        if keyword == "CREATE":
            remainder = _command_remainder(statement)
            if remainder.startswith("EXTENSION"):
                return "CREATE EXTENSION"
            if remainder.startswith("FUNCTION"):
                return "CREATE FUNCTION"
            if remainder.startswith("PROCEDURE"):
                return "CREATE PROCEDURE"
            if remainder.startswith("ROLE"):
                return "CREATE ROLE"
            if remainder.startswith("USER"):
                return "CREATE USER"

    # exp.Alias is used by sqlglot for some bare keyword forms that it
    # cannot classify as commands (e.g. LISTEN chan, NOTIFY chan).
    if isinstance(statement, exp.Alias):
        keyword = _alias_keyword(statement)
        if keyword in _ALIAS_BLOCKLIST:
            return keyword

    return None


# ---------------------------------------------------------------------------
# Read-only bypass detection: patterns that are syntactically SELECT-like but
# perform mutations or acquire exclusive locks.
# ---------------------------------------------------------------------------

# CTE body node types whose presence makes a CTE "data-modifying".
_MODIFYING_CTE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)


def classify_readonly_bypass(statement: exp.Expression) -> str | None:  # noqa: C901
    """Detect SELECT-adjacent forms that violate strict read-only posture.

    Checks (in order):
    1. Data-modifying CTEs — any CTE body containing INSERT/UPDATE/DELETE/MERGE
       or a statement already classified as blocked (DO, COPY, CALL, …).
    2. SELECT INTO — Postgres-style ``SELECT … INTO new_table …``.
    3. Locking clauses — ``FOR UPDATE``, ``FOR SHARE``, ``FOR NO KEY UPDATE``,
       ``FOR KEY SHARE``.

    Returns a short label string describing the violation, or ``None`` when the
    statement passes all read-only checks.
    """
    # 1. Data-modifying CTEs ---------------------------------------------------
    for with_node in statement.find_all(exp.With):
        for cte in with_node.expressions:
            # Each CTE is an exp.CTE whose .this is the body expression.
            body = cte.this if isinstance(cte, exp.CTE) else cte
            if body is None:
                continue
            # Direct modifying node types.
            if isinstance(body, _MODIFYING_CTE_TYPES):
                return "MODIFYING_CTE"
            # Recursively blocked statements (COPY, DO, CALL, …).
            if classify_blocked_statement(body) is not None:
                return "MODIFYING_CTE"
            # Nested modifying nodes inside the CTE body.
            for child in body.walk():
                if isinstance(child, _MODIFYING_CTE_TYPES):
                    return "MODIFYING_CTE"

    # 2. SELECT INTO -----------------------------------------------------------
    # sqlglot represents Postgres SELECT...INTO as exp.Select with an "into"
    # argument, OR as a top-level exp.Create with SELECT source, depending on
    # the dialect.  Cover both representations.
    for select_node in statement.find_all(exp.Select):
        if select_node.args.get("into"):
            return "SELECT INTO"

    # 3. Locking clauses (FOR UPDATE / FOR SHARE / …) -------------------------
    for lock_node in statement.find_all(exp.Lock):
        _ = lock_node  # presence alone is sufficient
        return "LOCKING_CLAUSE"

    return None


def classify_sql_policy_violation(statement: exp.Expression) -> SQLPolicyViolation | None:
    """Return the first shared SQL policy violation detected in a statement AST.

    Classification hierarchy:
    1. ``blocked_statement``  — explicit high-risk statement type (COPY, DO, …)
    2. ``statement_type_not_allowed`` — any non-SELECT/WITH/set-op statement
    3. ``readonly_violation``  — SELECT-adjacent forms that mutate or lock
    4. ``blocked_function``   — function call on the denylist

    Both the Agent ``PolicyEnforcer`` and the MCP ``_validate_sql_ast_failure``
    call this routine so classification is always identical across layers.
    """
    blocked_statement = classify_blocked_statement(statement)
    if blocked_statement:
        return SQLPolicyViolation(
            reason_code="blocked_statement",
            error_code=SQL_FORBIDDEN_STATEMENT_CODE,
            statement=blocked_statement,
        )

    if statement.key not in ALLOWED_STATEMENT_TYPES:
        return SQLPolicyViolation(
            reason_code="statement_type_not_allowed",
            error_code=SQL_FORBIDDEN_STATEMENT_CODE,
            statement=str(statement.key).upper(),
        )

    readonly_bypass = classify_readonly_bypass(statement)
    if readonly_bypass:
        return SQLPolicyViolation(
            reason_code="readonly_violation",
            error_code=SQL_READONLY_VIOLATION,
            statement=readonly_bypass,
        )

    for node in statement.find_all(exp.Func):
        for function_name in extract_function_names(node):
            if function_name in BLOCKED_FUNCTIONS:
                return SQLPolicyViolation(
                    reason_code="blocked_function",
                    error_code=SQL_FORBIDDEN_FUNCTION_CODE,
                    function=function_name,
                )

    return None


def _parse_csv(value: str) -> set[str]:
    out = set()
    for part in (value or "").split(","):
        normalized = part.strip().lower()
        if normalized:
            out.add(normalized)
    return out


def get_blocked_tables() -> set[str]:
    """Return normalized blocked tables from env override or default policy."""
    configured = _parse_csv(get_env_str("SQL_BLOCKED_TABLES", ""))
    if configured:
        return configured
    return set(DEFAULT_BLOCKED_TABLES)


def get_blocked_schemas() -> set[str]:
    """Return normalized blocked schemas from env override or default policy."""
    configured = _parse_csv(get_env_str("SQL_BLOCKED_SCHEMAS", ""))
    if configured:
        return configured
    return set(DEFAULT_BLOCKED_SCHEMAS)


def load_policy_snapshot() -> Dict[str, Any]:
    """Create a deterministic snapshot of the current policy configuration.

    Returns:
        Dictionary containing policy configuration state.
    """
    return {
        "snapshot_id": f"policy-v1-{int(time.time())}",
        "blocked_tables": list(get_blocked_tables()),
        "blocked_schemas": list(get_blocked_schemas()),
        "timestamp": time.time(),
    }


def classify_blocked_table_reference(
    *,
    table_name: str,
    schema_name: str = "",
    snapshot: Optional[Dict[str, Any]] = None,
) -> str | None:
    """Classify blocked table/schema references from normalized identifiers.

    Args:
        table_name: Normalized table name.
        schema_name: Normalized schema name.
        snapshot: Optional policy snapshot dictionary. If None, loads live policy.

    Returns:
        Reason code when blocked, otherwise None.
    """
    normalized_table = str(table_name or "").strip().lower()
    normalized_schema = str(schema_name or "").strip().lower()

    if not normalized_table:
        return None

    if snapshot:
        blocked_tables = set(snapshot.get("blocked_tables", []))
        blocked_schemas = set(snapshot.get("blocked_schemas", []))
    else:
        blocked_tables = get_blocked_tables()
        blocked_schemas = get_blocked_schemas()

    if normalized_table in blocked_tables:
        return "restricted_table"

    if normalized_schema and normalized_schema in blocked_schemas:
        return "blocked_schema"

    if normalized_table.startswith(BLOCKED_TABLE_PREFIXES):
        return "system_table"

    full_name = f"{normalized_schema}.{normalized_table}" if normalized_schema else normalized_table
    if full_name.startswith("information_schema."):
        return "system_table"

    return None

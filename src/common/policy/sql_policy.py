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


def classify_blocked_statement(statement: exp.Expression) -> str | None:
    """Classify explicit high-risk statements that must always be rejected."""
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
        if kind in {"FUNCTION", "PROCEDURE"}:
            return f"CREATE {kind}"

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
        if keyword == "CREATE":
            remainder = _command_remainder(statement)
            if remainder.startswith("FUNCTION"):
                return "CREATE FUNCTION"
            if remainder.startswith("PROCEDURE"):
                return "CREATE PROCEDURE"
            if remainder.startswith("EXTENSION"):
                return "CREATE EXTENSION"
            if remainder.startswith("ROLE"):
                return "CREATE ROLE"

    if isinstance(statement, exp.Alias):
        keyword = _alias_keyword(statement)
        if keyword in _ALIAS_BLOCKLIST:
            return keyword

    return None


def classify_sql_policy_violation(statement: exp.Expression) -> SQLPolicyViolation | None:
    """Return the first shared SQL policy violation detected in a statement AST."""
    blocked_statement = classify_blocked_statement(statement)
    if blocked_statement:
        return SQLPolicyViolation(
            reason_code="blocked_statement",
            statement=blocked_statement,
        )

    if statement.key not in ALLOWED_STATEMENT_TYPES:
        return SQLPolicyViolation(
            reason_code="statement_type_not_allowed",
            statement=str(statement.key).upper(),
        )

    for node in statement.find_all(exp.Func):
        for function_name in extract_function_names(node):
            if function_name in BLOCKED_FUNCTIONS:
                return SQLPolicyViolation(
                    reason_code="blocked_function",
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

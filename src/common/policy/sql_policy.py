"""Centralized SQL policy configuration.

Defines SQL statement, function, and sensitive-column policies shared by
Agent and MCP validation layers.
"""

from typing import Set

from common.config.env import get_env_str

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
    "pg_sleep",
    "sleep",
    "usleep",
    "sys_sleep",
    "pg_read_file",
    "pg_ls_dir",
    "pg_stat_file",
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


def is_sensitive_column_name(column_name: str) -> bool:
    """Return True when a column name matches a sensitive marker."""
    if not isinstance(column_name, str):
        return False
    normalized = column_name.strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in SENSITIVE_COLUMN_NAME_PATTERNS)


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


def classify_blocked_table_reference(
    *,
    table_name: str,
    schema_name: str = "",
) -> str | None:
    """Classify blocked table/schema references from normalized identifiers.

    Returns:
        Reason code when blocked, otherwise None.
    """
    normalized_table = str(table_name or "").strip().lower()
    normalized_schema = str(schema_name or "").strip().lower()

    if not normalized_table:
        return None

    if normalized_table in get_blocked_tables():
        return "restricted_table"

    if normalized_schema and normalized_schema in get_blocked_schemas():
        return "blocked_schema"

    if normalized_table.startswith(BLOCKED_TABLE_PREFIXES):
        return "system_table"

    full_name = f"{normalized_schema}.{normalized_table}" if normalized_schema else normalized_table
    if full_name.startswith("information_schema."):
        return "system_table"

    return None

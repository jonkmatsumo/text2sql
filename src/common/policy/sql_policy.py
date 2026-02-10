"""Centralized SQL policy configuration.

Defines SQL statement, function, and sensitive-column policies shared by
Agent and MCP validation layers.
"""

from typing import Set

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

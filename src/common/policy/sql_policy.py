"""Centralized SQL policy configuration.

Defines allowed statement types and blocked functions used by both
the Agent and MCP Server SQL validation layers.
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

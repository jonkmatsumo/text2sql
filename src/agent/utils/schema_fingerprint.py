"""Schema fingerprint utilities for snapshot versioning."""

import hashlib
import json
from typing import Any

from common.config.env import get_env_str


def canonicalize_schema_nodes(nodes: list[dict]) -> list[dict]:
    """Return a deterministic schema representation from graph nodes."""
    tables: dict[str, list[dict[str, Any]]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") == "Table":
            name = str(node.get("name", "")).strip()
            if name:
                tables.setdefault(name, [])

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "Column":
            continue
        table_name = str(node.get("table", "")).strip()
        column_name = str(node.get("name", "")).strip()
        if not table_name or not column_name:
            continue
        col_type = node.get("data_type") or node.get("type") or node.get("db_type") or "unknown"
        col_type = str(col_type)
        tables.setdefault(table_name, []).append({"name": column_name, "type": col_type})

    canonical_tables = []
    for table_name in sorted(tables.keys()):
        columns = tables[table_name]
        sorted_columns = sorted(columns, key=lambda c: (c["name"], c["type"]))
        canonical_tables.append({"table": table_name, "columns": sorted_columns})

    return canonical_tables


def fingerprint_schema_nodes(nodes: list[dict]) -> str:
    """Compute a deterministic fingerprint for schema nodes."""
    canonical = canonicalize_schema_nodes(nodes)
    payload = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def resolve_schema_snapshot_id(nodes: list[dict]) -> str:
    """Resolve schema snapshot id based on configured mode."""
    mode = get_env_str("SCHEMA_SNAPSHOT_MODE", "fingerprint").strip().lower()
    if mode == "static":
        return "v1.0"
    if mode == "fingerprint":
        return f"fp-{fingerprint_schema_nodes(nodes)}"
    return "unknown"


# =============================================================================
# Pre-Execution Schema Validation
# =============================================================================


def _pre_exec_validation_enabled() -> bool:
    """Check if pre-execution schema validation is enabled."""
    from common.config.env import get_env_bool

    return get_env_bool("AGENT_PRE_EXEC_SCHEMA_VALIDATION", True) is True


def extract_sql_tables(sql: str) -> frozenset[str]:
    """Extract table names from SQL query.

    Args:
        sql: SQL query string

    Returns:
        frozenset of lowercase table names, empty set if parsing fails
    """
    if not sql:
        return frozenset()

    try:
        import sqlglot
        from sqlglot import exp

        ast = sqlglot.parse_one(sql)
        tables = set()
        for table in ast.find_all(exp.Table):
            if table.name:
                tables.add(table.name.lower())
        return frozenset(tables)
    except Exception:
        return frozenset()


def validate_sql_against_schema(
    sql: str,
    schema_context: list[dict],
) -> tuple[bool, frozenset[str], str | None]:
    """Validate SQL tables against schema context.

    Args:
        sql: SQL query string
        schema_context: List of schema context dicts from raw_schema_context

    Returns:
        Tuple of (passed, missing_tables, warning_message)
    """
    if not _pre_exec_validation_enabled():
        return (True, frozenset(), None)

    if not schema_context:
        # No schema context - can't validate, pass through
        return (True, frozenset(), None)

    sql_tables = extract_sql_tables(sql)
    if not sql_tables:
        # No tables found or parse failed - pass through
        return (True, frozenset(), None)

    # Build set of known table names from schema context
    known_tables: set[str] = set()
    for item in schema_context:
        if isinstance(item, dict):
            table_name = item.get("table_name") or item.get("name") or item.get("table")
            if table_name:
                known_tables.add(str(table_name).lower())

    # Check for tables in SQL that aren't in schema context
    missing = sql_tables - known_tables

    if missing:
        missing_list = sorted(missing)[:5]
        warning = (
            f"Pre-execution validation: {len(missing)} table(s) not in schema context: "
            f"{', '.join(missing_list)}"
        )
        if len(missing) > 5:
            warning += f" (+{len(missing) - 5} more)"
        return (False, frozenset(missing), warning)

    return (True, frozenset(), None)

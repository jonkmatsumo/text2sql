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


def extract_sql_identifiers(sql: str) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
    """Extract referenced tables and qualified columns from SQL query."""
    if not sql:
        return frozenset(), frozenset()

    try:
        import sqlglot
        from sqlglot import exp

        ast = sqlglot.parse_one(sql)
        tables = set()
        columns = set()
        for table in ast.find_all(exp.Table):
            if table.name:
                tables.add(table.name.lower())
        for column in ast.find_all(exp.Column):
            # Restrict to qualified refs to avoid alias/implicit-column false positives.
            if column.table and column.name:
                columns.add((column.table.lower(), column.name.lower()))
        return frozenset(tables), frozenset(columns)
    except Exception:
        return frozenset(), frozenset()


def extract_sql_tables(sql: str) -> frozenset[str]:
    """Backward-compatible table extraction helper."""
    tables, _ = extract_sql_identifiers(sql)
    return tables


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

    sql_tables, sql_columns = extract_sql_identifiers(sql)
    if not sql_tables:
        # No tables found or parse failed - pass through
        return (True, frozenset(), None)

    # Build set of known table/column names from schema context
    known_tables: set[str] = set()
    known_columns: dict[str, set[str]] = {}
    for item in schema_context:
        if isinstance(item, dict):
            item_type = str(item.get("type") or "").lower()
            if item_type == "column":
                table_name = item.get("table")
                column_name = item.get("name")
                if table_name and column_name:
                    normalized_table = str(table_name).lower()
                    known_tables.add(normalized_table)
                    known_columns.setdefault(normalized_table, set()).add(str(column_name).lower())
            else:
                table_name = item.get("table_name") or item.get("name") or item.get("table")
                if table_name:
                    normalized_table = str(table_name).lower()
                    known_tables.add(normalized_table)

    # Check for tables in SQL that aren't in schema context
    missing_tables = sql_tables - known_tables
    missing_columns = {
        f"{table}.{column}"
        for table, column in sql_columns
        if table in known_columns and column not in known_columns[table]
    }
    missing = set(missing_tables) | missing_columns

    if missing:
        missing_list = sorted(missing)[:5]
        warning = f"Pre-execution validation: {len(missing)} identifier(s) not in schema context: "
        warning += ", ".join(missing_list)
        if len(missing) > 5:
            warning += f" (+{len(missing) - 5} more)"
        return (False, frozenset(sorted(missing)), warning)

    return (True, frozenset(), None)

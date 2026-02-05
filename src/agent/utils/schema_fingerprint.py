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

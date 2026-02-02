from __future__ import annotations

import re


def normalize_type_for_display(type_name: str) -> str:
    """Normalize a type name for display-only contexts."""
    if not type_name:
        return type_name

    normalized = type_name.strip().lower()
    if normalized.startswith("character varying"):
        return "string"

    base = re.split(r"[\s(]", normalized, maxsplit=1)[0]

    if normalized.startswith("tinyint(1)"):
        return "boolean"
    if base in {"bool", "boolean"}:
        return "boolean"
    if base in {"int", "integer", "int4", "int32", "smallint", "tinyint"}:
        return "int"
    if base in {"int8", "bigint", "int64"}:
        return "bigint"
    if base in {"float", "float8", "double", "real", "float64"}:
        return "float"
    if base in {"numeric", "decimal", "number"}:
        return "decimal"
    if base in {"varchar", "text", "string", "char"}:
        return "string"
    if base in {"timestamp", "timestamptz", "timestamp_ntz", "datetime"}:
        return "timestamp"
    if base == "date":
        return "date"
    if base in {"json", "jsonb"}:
        return "json"
    if base in {"bytea", "blob", "binary"}:
        return "binary"

    return type_name

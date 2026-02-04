from __future__ import annotations

from typing import Any, Optional

LogicalType = str

LOGICAL_TYPES = {
    "integer",
    "float",
    "numeric",
    "boolean",
    "string",
    "timestamp",
    "date",
    "time",
    "json",
    "uuid",
    "unknown",
}


def logical_type_from_db_type(
    db_type: Optional[str], provider: Optional[str] = None
) -> LogicalType:
    """Map a provider-specific db type string to a logical type."""
    if not db_type:
        return "unknown"
    normalized = db_type.strip().lower()
    if "timestamp" in normalized:
        return "timestamp"
    if normalized == "date" or normalized.endswith(" date"):
        return "date"
    if normalized == "time" or normalized.endswith(" time"):
        return "time"
    if "bool" in normalized:
        return "boolean"
    if "uuid" in normalized:
        return "uuid"
    if "json" in normalized:
        return "json"
    if "numeric" in normalized or "decimal" in normalized:
        return "numeric"
    if any(token in normalized for token in ("double", "float", "real")):
        return "float"
    if "int" in normalized:
        return "integer"
    if any(token in normalized for token in ("char", "text", "string", "varchar")):
        return "string"
    if provider == "sqlite":
        if "blob" in normalized:
            return "unknown"
    return "unknown"


def logical_type_from_asyncpg_oid(oid: int) -> LogicalType:
    """Map asyncpg OIDs to logical types."""
    oid_mapping = {
        16: "boolean",
        20: "integer",
        21: "integer",
        23: "integer",
        700: "float",
        701: "float",
        1700: "numeric",
        1082: "date",
        1083: "time",
        1114: "timestamp",
        1184: "timestamp",
        114: "json",
        3802: "json",
        2950: "uuid",
        25: "string",
        1043: "string",
    }
    return oid_mapping.get(int(oid), "unknown")


def logical_type_from_cursor_description(
    desc_entry: Any, provider: Optional[str] = None
) -> LogicalType:
    """Map cursor description entry to a logical type when possible."""
    if desc_entry is None:
        return "unknown"
    if isinstance(desc_entry, dict):
        return logical_type_from_db_type(
            desc_entry.get("type") or desc_entry.get("db_type"), provider
        )
    if hasattr(desc_entry, "type_code"):
        type_code = getattr(desc_entry, "type_code")
        if isinstance(type_code, str):
            return logical_type_from_db_type(type_code, provider)
        if isinstance(type_code, int) and provider == "mysql":
            return _logical_type_from_mysql_type_code(type_code)
    if isinstance(desc_entry, (list, tuple)) and len(desc_entry) > 1:
        type_code = desc_entry[1]
        if isinstance(type_code, str):
            return logical_type_from_db_type(type_code, provider)
        if isinstance(type_code, int) and provider == "mysql":
            return _logical_type_from_mysql_type_code(type_code)
    return "unknown"


def _logical_type_from_mysql_type_code(type_code: int) -> LogicalType:
    mysql_mapping = {
        0: "integer",  # DECIMAL
        1: "integer",  # TINY
        2: "integer",  # SHORT
        3: "integer",  # LONG
        4: "float",  # FLOAT
        5: "float",  # DOUBLE
        8: "integer",  # LONGLONG
        9: "integer",  # INT24
        10: "date",  # DATE
        11: "time",  # TIME
        12: "timestamp",  # DATETIME
        13: "date",  # YEAR
        246: "numeric",  # NEWDECIMAL
        7: "timestamp",  # TIMESTAMP
        252: "string",  # BLOB/TEXT
        253: "string",  # VAR_STRING
        254: "string",  # STRING
    }
    return mysql_mapping.get(type_code, "unknown")

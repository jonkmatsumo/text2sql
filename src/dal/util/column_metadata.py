"""Helpers for building column metadata payloads."""

from __future__ import annotations

from typing import Any, List, Optional

from dal.util.logical_types import (
    logical_type_from_asyncpg_oid,
    logical_type_from_cursor_description,
    logical_type_from_db_type,
)


def build_column_meta(
    name: str,
    logical_type: str,
    db_type: Optional[str] = None,
    nullable: Optional[bool] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[str] = None,
) -> dict:
    """Return a normalized column metadata payload."""
    return {
        "name": name,
        "type": logical_type,
        "db_type": db_type,
        "nullable": nullable,
        "precision": precision,
        "scale": scale,
        "timezone": timezone,
    }


def columns_from_asyncpg_attributes(attrs: List[Any]) -> List[dict]:
    """Build column metadata from asyncpg statement attributes."""
    columns: List[dict] = []
    for attr in attrs or []:
        name = getattr(attr, "name", None) or str(attr)
        db_type = None
        oid = None
        attr_type = getattr(attr, "type", None)
        if attr_type is not None:
            db_type = getattr(attr_type, "name", None)
            oid = getattr(attr_type, "oid", None)
        logical_type = (
            logical_type_from_asyncpg_oid(oid)
            if oid is not None
            else logical_type_from_db_type(db_type)
        )
        columns.append(build_column_meta(name, logical_type, db_type=db_type))
    return columns


def columns_from_cursor_description(
    description: Optional[list], provider: Optional[str] = None
) -> List[dict]:
    """Build column metadata from DB-API cursor description tuples."""
    columns: List[dict] = []
    for entry in description or []:
        name = (
            entry[0] if isinstance(entry, (list, tuple)) and entry else getattr(entry, "name", None)
        )
        logical_type = logical_type_from_cursor_description(entry, provider=provider)
        db_type = None
        if isinstance(entry, (list, tuple)) and len(entry) > 1 and isinstance(entry[1], str):
            db_type = entry[1]
        columns.append(build_column_meta(name, logical_type, db_type=db_type))
    return columns

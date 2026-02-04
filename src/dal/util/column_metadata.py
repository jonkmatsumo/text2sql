"""Helpers for building column metadata payloads."""

from __future__ import annotations

from typing import Any, List, Optional

from dal.util.logical_types import logical_type_from_asyncpg_oid, logical_type_from_db_type


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

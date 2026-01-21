"""Database schema definitions."""

from typing import List, Optional

from pydantic import BaseModel


class ColumnDef(BaseModel):
    """Definition of a table column."""

    name: str
    data_type: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: Optional[str] = None  # Format: "table(column)"


class ForeignKeyDef(BaseModel):
    """Definition of a foreign key constraint."""

    column: str
    ref_table: str
    ref_column: str


class TableDef(BaseModel):
    """Definition of a database table."""

    name: str
    columns: List[ColumnDef]
    foreign_keys: List[ForeignKeyDef] = []

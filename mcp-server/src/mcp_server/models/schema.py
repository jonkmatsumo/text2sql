from typing import Dict, List, Optional

from pydantic import BaseModel


class TableMetadata(BaseModel):
    """Metadata for a database table."""

    name: str
    description: Optional[str] = None
    sample_data: List[Dict] = []


class ColumnMetadata(BaseModel):
    """Metadata for a table column."""

    name: str
    type: str
    is_primary_key: bool = False
    description: Optional[str] = None


class ForeignKey(BaseModel):
    """Foreign key relationship definition."""

    source_col: str
    target_table: str
    target_col: str

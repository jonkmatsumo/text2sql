from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .column_def import ColumnDef
from .foreign_key_def import ForeignKeyDef


class TableDef(BaseModel):
    """Canonical representation of a database table definition."""

    name: str
    columns: List[ColumnDef] = Field(default_factory=list)
    foreign_keys: List[ForeignKeyDef] = Field(default_factory=list)
    description: Optional[str] = None
    sample_data: List[Dict] = []

    model_config = {"frozen": False}

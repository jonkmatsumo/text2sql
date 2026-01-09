from typing import Optional

from pydantic import BaseModel


class ColumnDef(BaseModel):
    """Canonical representation of a database column definition."""

    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    description: Optional[str] = None

    model_config = {"frozen": False}

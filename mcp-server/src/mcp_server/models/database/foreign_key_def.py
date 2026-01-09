from pydantic import BaseModel


class ForeignKeyDef(BaseModel):
    """Canonical representation of a foreign key constraint."""

    column_name: str
    foreign_table_name: str
    foreign_column_name: str

    model_config = {"frozen": False}

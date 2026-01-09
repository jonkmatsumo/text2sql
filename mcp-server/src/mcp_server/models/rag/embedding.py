from typing import List

from pydantic import BaseModel


class SchemaEmbedding(BaseModel):
    """Canonical representation of a table schema embedding.

    Attributes:
        table_name: Name of the table.
        schema_text: Text description of the schema (columns, FKs).
        embedding: The embedding vector of the schema text.
    """

    table_name: str
    schema_text: str
    embedding: List[float]

    model_config = {"frozen": False}

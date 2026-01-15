from typing import List

from pydantic import BaseModel


class Example(BaseModel):
    """Canonical representation of a few-shot learning example.

    Attributes:
        id: Unique identifier.
        question: The natural language question.
        sql_query: The corresponding SQL query.
        embedding: The embedding vector of the question.
    """

    id: int
    question: str
    sql_query: str
    embedding: List[float]

    model_config = {"frozen": False}

from typing import Optional

from mcp_server.retrievers.data_schema_retriever import DataSchemaRetriever
from mcp_server.retrievers.postgres_retriever import PostgresRetriever

_retriever_instance: Optional[DataSchemaRetriever] = None


def get_retriever() -> DataSchemaRetriever:
    """
    Get the singleton instance of the DataSchemaRetriever.

    Initializes a PostgresRetriever if no instance exists.
    """
    global _retriever_instance
    if _retriever_instance is None:
        # PostgresRetriever will automatically pick up DATABASE_URL from env
        _retriever_instance = PostgresRetriever()
    return _retriever_instance

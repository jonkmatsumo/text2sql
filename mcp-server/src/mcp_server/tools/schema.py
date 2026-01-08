import json
from typing import List, Optional

from mcp_server.factory.retriever import get_retriever

# Since we are integrating these tools into the main server.py,
# we define functions that can be registered or decorating them if we had the app instance here.
# But conventionally in this project, tools are defined and then registered.


def list_tables(search_term: Optional[str] = None, tenant_id: Optional[int] = None) -> str:
    """
    List all tables in the database with their descriptions.

    Args:
        search_term: Optional fuzzy search string to filter table names.
        tenant_id: Optional tenant identifier (unused for schema retrieval).

    Returns:
        JSON string of table metadata.
    """
    retriever = get_retriever()
    tables = retriever.list_tables()

    results = []
    for t in tables:
        if search_term and search_term.lower() not in t.name.lower():
            continue
        results.append(t.model_dump())

    return json.dumps(results, separators=(",", ":"))


def get_table_schema(table_names: List[str], tenant_id: Optional[int] = None) -> str:
    """
    Get detailed schema for a specific table.

    Args:
        table_names: List of table names to inspect.
        tenant_id: Optional tenant identifier (unused).

    Returns:
        JSON string of table schemas.
    """
    retriever = get_retriever()
    results = []

    for table_name in table_names:
        # Note: PostgresRetriever expects single table name
        cols = retriever.get_columns(table_name)
        fks = retriever.get_foreign_keys(table_name)

        results.append(
            {
                "table_name": table_name,
                "columns": [c.model_dump() for c in cols],
                "foreign_keys": [fk.model_dump() for fk in fks],
            }
        )

    return json.dumps(results, separators=(",", ":"))


def get_sample_data(table_name: str, limit: int = 3, tenant_id: Optional[int] = None) -> str:
    """
    Get sample data rows from a table.

    Args:
        table_name: The name of the table.
        limit: Number of rows to return (default: 3).
        tenant_id: Optional tenant identifier (unused).

    Returns:
        JSON string of sample data.
    """
    retriever = get_retriever()
    data = retriever.get_sample_rows(table_name, limit)
    return json.dumps(data, separators=(",", ":"), default=str)

"""MCP tool: search_relevant_tables - Search for tables using semantic similarity."""

import json
from typing import Optional

from mcp_server.services.rag.engine import RagEngine, search_similar_tables

from dal.database import Database

TOOL_NAME = "search_relevant_tables"


async def handler(user_query: str, limit: int = 5, tenant_id: Optional[int] = None) -> str:
    """Search for tables relevant to a natural language query using semantic similarity.

    This tool solves the context window problem by returning only the most relevant
    table schemas instead of the entire database schema.

    Args:
        user_query: Natural language question (e.g., "Show me customer payments")
        limit: Maximum number of relevant tables to return (default: 5)
        tenant_id: Optional tenant identifier (not required for schema queries).

    Returns:
        JSON array of relevant tables with schema information.
    """
    # Generate embedding for user query
    query_embedding = RagEngine.embed_text(user_query)

    # Search for similar tables
    results = await search_similar_tables(query_embedding, limit=limit, tenant_id=tenant_id)

    structured_results = []
    introspector = Database.get_schema_introspector()

    for result in results:
        table_name = result["table_name"]

        try:
            table_def = await introspector.get_table_def(table_name)

            table_columns = [
                {
                    "name": col.name,
                    "type": col.data_type,
                    "required": not col.is_nullable,
                }
                for col in table_def.columns
            ]

            structured_results.append(
                {
                    "table_name": table_name,
                    "description": result["schema_text"],
                    "similarity": 1 - result["distance"],
                    "columns": table_columns,
                }
            )
        except Exception:
            # Skip tables that might be in index but missing in introspection
            continue

    return json.dumps(structured_results, separators=(",", ":"))

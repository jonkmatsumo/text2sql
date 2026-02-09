"""MCP tool: search_relevant_tables - Search for tables using semantic similarity."""

from typing import Optional

from dal.database import Database
from mcp_server.services.rag.engine import RagEngine, search_similar_tables

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
    import time

    from mcp_server.utils.validation import validate_limit

    if err := validate_limit(limit, TOOL_NAME, min_val=1, max_val=50):
        return err

    start_time = time.monotonic()

    # Generate embedding for user query
    query_embedding = await RagEngine.embed_text(user_query)

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
        except Exception as e:
            # Skip tables that might be in index but missing in introspection
            structured_results.append(
                {
                    "table_name": table_name,
                    "status": "error",
                    "error": f"Introspection failed: {str(e)}",
                }
            )

    import time

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    envelope = ToolResponseEnvelope(
        result=structured_results,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
        ),
    )
    return envelope.model_dump_json(exclude_none=True)

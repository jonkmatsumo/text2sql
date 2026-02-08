"""MCP tool: get_few_shot_examples - Retrieve similar past queries and SQL."""

from mcp_server.services.rag import get_relevant_examples

TOOL_NAME = "get_few_shot_examples"


async def handler(query: str, tenant_id: int, limit: int = 3) -> str:
    """Retrieve similar past queries and their corresponding SQL from the registry.

    Use this tool to find examples of how to write SQL for similar questions.

    Args:
        query: The user query to find similar examples for.
        tenant_id: Tenant identifier (REQUIRED).
        limit: Maximum number of examples to return (default: 3).

    Returns:
        JSON string with similar query-SQL pairs.
    """
    import json

    if tenant_id is None:
        return json.dumps(
            {
                "error": "Tenant ID is required for get_few_shot_examples.",
                "error_category": "invalid_request",
            }
        )
    return await get_relevant_examples(query, limit, tenant_id)

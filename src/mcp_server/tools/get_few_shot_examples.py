"""MCP tool: get_few_shot_examples - Retrieve similar past queries and SQL."""

from mcp_server.services.rag import get_relevant_examples

TOOL_NAME = "get_few_shot_examples"
TOOL_DESCRIPTION = "Retrieve similar past queries and their corresponding SQL from the registry."


async def handler(query: str, tenant_id: int, limit: int = 3) -> str:
    """Retrieve similar past queries and their corresponding SQL from the registry.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher) and valid 'tenant_id'.

    Data Access:
        Read-only access to the few-shot Registry store. Results are scoped by tenant_id.

    Failure Modes:
        - Unauthorized: If tenant_id is missing or role is insufficient.
        - Validation Error: If limit is out of bounds.
        - RAG Error: If the similarity search fails.

    Args:
        query: The user query to find similar examples for.
        tenant_id: Tenant identifier (REQUIRED).
        limit: Maximum number of examples to return (default: 3).

    Returns:
        JSON string with similar query-SQL pairs.
    """
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    from mcp_server.utils.validation import validate_limit

    if err := validate_limit(limit, TOOL_NAME):
        return err
    from common.models.tool_envelopes import GenericToolMetadata, GenericToolResponseEnvelope

    examples = await get_relevant_examples(query, tenant_id=tenant_id, limit=limit)

    return GenericToolResponseEnvelope(
        result=examples,
        metadata=GenericToolMetadata(provider="registry"),
    ).model_dump_json()

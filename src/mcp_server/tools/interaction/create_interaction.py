"""MCP tool: create_interaction - Log the start of a user interaction."""

from typing import Optional

from dal.factory import get_interaction_store

TOOL_NAME = "create_interaction"


async def handler(
    conversation_id: Optional[str],
    schema_snapshot_id: str,
    user_nlq_text: str,
    tenant_id: int = 1,
    model_version: Optional[str] = None,
    prompt_version: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> str:
    """Log the start of a user interaction.

    Args:
        conversation_id: Optional conversation identifier for grouping.
        schema_snapshot_id: Identifier for the schema snapshot used.
        user_nlq_text: The user's natural language query.
        tenant_id: Tenant identifier (default: 1).
        model_version: Optional model version used for generation.
        prompt_version: Optional prompt version used.
        trace_id: Optional trace identifier for debugging.

    Returns:
        interaction_id (UUID string)
    """
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    store = get_interaction_store()
    return await store.create_interaction(
        conversation_id=conversation_id,
        schema_snapshot_id=schema_snapshot_id,
        user_nlq_text=user_nlq_text,
        tenant_id=tenant_id,
        model_version=model_version,
        prompt_version=prompt_version,
        trace_id=trace_id,
    )

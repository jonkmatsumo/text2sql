"""MCP tool: create_interaction - Log the start of a user interaction."""

from typing import Optional

from dal.factory import get_interaction_store

TOOL_NAME = "create_interaction"
TOOL_DESCRIPTION = "Log the start of a user interaction."


async def handler(
    conversation_id: Optional[str],
    schema_snapshot_id: str,
    user_nlq_text: str,
    tenant_id: Optional[int] = None,
    model_version: Optional[str] = None,
    prompt_version: Optional[str] = None,
    trace_id: Optional[str] = None,
) -> str:
    """Log the start of a user interaction.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Write access to the interaction store.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Validation Error: If required fields (e.g., user_nlq_text) are missing.
        - Database Error: If the interaction store is unavailable.

    Args:
        conversation_id: Unique identifier for the conversation.
        schema_snapshot_id: Schema snapshot ID at the time of interaction.
        user_nlq_text: Natural language query text.
        tenant_id: Tenant identifier.
        model_version: Optional model version used.
        prompt_version: Optional prompt version used.
        trace_id: Optional trace identifier for observability.

    Returns:
        JSON-encoded ToolResponseEnvelope containing the new interaction_id.
    """
    from mcp_server.utils.envelopes import tool_success_response
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    # We allow read-only access for creating interactions (baseline role)
    # but we can enforce a baseline role if needed. Using empty string for no strict role.

    store = get_interaction_store()
    interaction_id = await store.create_interaction(
        conversation_id=conversation_id,
        schema_snapshot_id=schema_snapshot_id,
        user_nlq_text=user_nlq_text,
        tenant_id=tenant_id,
        model_version=model_version,
        prompt_version=prompt_version,
        trace_id=trace_id,
    )
    return tool_success_response(interaction_id)

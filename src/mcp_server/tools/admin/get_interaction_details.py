"""MCP tool: get_interaction_details - Get full details for an interaction."""

from dal.factory import get_feedback_store, get_interaction_store

TOOL_NAME = "get_interaction_details"
TOOL_DESCRIPTION = "Get full details for a specific interaction, including all feedback."


async def handler(interaction_id: str) -> str:
    """Get full details for a specific interaction, including all feedback.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the interaction and feedback stores.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Not Found: If the interaction_id does not exist.
        - Database Error: If stores are unavailable.

    Args:
        interaction_id: The unique identifier of the interaction.

    Returns:
        JSON string containing interaction details and feedback list.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.errors import tool_error_response

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
        return err

    i_store = get_interaction_store()
    f_store = get_feedback_store()

    interaction = await i_store.get_interaction_detail(interaction_id)
    if not interaction:
        return tool_error_response(
            message=f"Interaction {interaction_id} not found",
            code="INTERACTION_NOT_FOUND",
            category="not_found",
            provider="interaction_store",
            retryable=False,
        )

    feedback = await f_store.get_feedback_for_interaction(interaction_id)
    interaction["feedback"] = feedback

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result=interaction,
        metadata=GenericToolMetadata(
            provider="interaction_store", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

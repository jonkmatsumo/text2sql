"""MCP tool: get_interaction_details - Get full details for an interaction."""

from dal.factory import get_feedback_store, get_interaction_store

TOOL_NAME = "get_interaction_details"


async def handler(interaction_id: str) -> str:
    """Get full details for a specific interaction, including all feedback.

    Args:
        interaction_id: The unique identifier of the interaction.

    Returns:
        Dictionary with interaction details and feedback list.
    """
    import time

    from common.models.error_metadata import ErrorMetadata
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    start_time = time.monotonic()

    i_store = get_interaction_store()
    f_store = get_feedback_store()

    interaction = await i_store.get_interaction_detail(interaction_id)
    if not interaction:
        return ToolResponseEnvelope(
            result={},
            error=ErrorMetadata(
                message=f"Interaction {interaction_id} not found",
                category="not_found",
                provider="interaction_store",
                is_retryable=False,
            ),
        ).model_dump_json(exclude_none=True)

    feedback = await f_store.get_feedback_for_interaction(interaction_id)
    interaction["feedback"] = feedback

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result=interaction,
        metadata=GenericToolMetadata(
            provider="interaction_store", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

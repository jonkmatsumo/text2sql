"""MCP tool: reject_interaction - Reject an interaction."""

from typing import Optional

from dal.factory import get_feedback_store

TOOL_NAME = "reject_interaction"


async def handler(
    interaction_id: str,
    reason: str = "CANNOT_FIX",
    reviewer_notes: Optional[str] = None,
) -> str:
    """Reject an interaction.

    Args:
        interaction_id: The unique identifier of the interaction.
        reason: Reason for rejection (default: "CANNOT_FIX").
        reviewer_notes: Optional notes from the reviewer.

    Returns:
        "OK" on success.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    start_time = time.monotonic()
    store = get_feedback_store()
    await store.update_review_status(
        interaction_id=interaction_id,
        status="REJECTED",
        resolution_type=reason,
        reviewer_notes=reviewer_notes,
    )

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result={"status": "OK"},
        metadata=GenericToolMetadata(
            provider="feedback_store", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

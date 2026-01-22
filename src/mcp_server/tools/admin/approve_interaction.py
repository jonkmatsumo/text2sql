"""MCP tool: approve_interaction - Approve an interaction."""

from typing import Optional

from dal.factory import get_feedback_store

TOOL_NAME = "approve_interaction"


async def handler(
    interaction_id: str,
    corrected_sql: Optional[str] = None,
    resolution_type: str = "APPROVED_AS_IS",
    reviewer_notes: Optional[str] = None,
) -> str:
    """Approve an interaction and update its review status.

    Args:
        interaction_id: The unique identifier of the interaction.
        corrected_sql: Optional corrected SQL if the original was wrong.
        resolution_type: Type of resolution (default: "APPROVED_AS_IS").
        reviewer_notes: Optional notes from the reviewer.

    Returns:
        "OK" on success.
    """
    store = get_feedback_store()
    await store.update_review_status(
        interaction_id=interaction_id,
        status="APPROVED",
        resolution_type=resolution_type,
        corrected_sql=corrected_sql,
        reviewer_notes=reviewer_notes,
    )
    return "OK"

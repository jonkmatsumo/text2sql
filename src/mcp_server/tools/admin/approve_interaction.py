"""MCP tool: approve_interaction - Approve an interaction."""

from typing import Optional

from dal.factory import get_feedback_store

TOOL_NAME = "approve_interaction"
TOOL_DESCRIPTION = "Approve an interaction and update its review status."


async def handler(
    interaction_id: str,
    corrected_sql: Optional[str] = None,
    resolution_type: str = "APPROVED_AS_IS",
    reviewer_notes: Optional[str] = None,
) -> str:
    """Approve an interaction and update its review status.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read/Write access to the feedback store to update interaction records.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Not Found: If the interaction_id does not exist.
        - Database Error: If the feedback store is unavailable.

    Args:
        interaction_id: The unique identifier of the interaction.
        corrected_sql: Optional corrected SQL if the original was wrong.
        resolution_type: Type of resolution (default: "APPROVED_AS_IS").
        reviewer_notes: Optional notes from the reviewer.

    Returns:
        JSON object with status "OK" on success.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    start_time = time.monotonic()

    from mcp_server.utils.auth import require_admin

    if err := require_admin(TOOL_NAME):
        return err

    store = get_feedback_store()
    await store.update_review_status(
        interaction_id=interaction_id,
        status="APPROVED",
        resolution_type=resolution_type,
        corrected_sql=corrected_sql,
        reviewer_notes=reviewer_notes,
    )

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result={"status": "OK"},
        metadata=GenericToolMetadata(
            provider="feedback_store", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

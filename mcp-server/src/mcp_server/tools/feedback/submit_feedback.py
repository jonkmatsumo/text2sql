"""MCP tool: submit_feedback - Submit user feedback for an interaction."""

from typing import Optional

from mcp_server.dal.factory import get_feedback_store

TOOL_NAME = "submit_feedback"


async def handler(interaction_id: str, thumb: str, comment: Optional[str] = None) -> str:
    """Submit user feedback (UP/DOWN) for a specific interaction.

    Args:
        interaction_id: The unique identifier of the interaction.
        thumb: Feedback type - "UP" for positive, "DOWN" for negative.
        comment: Optional comment with the feedback.

    Returns:
        "OK" on success.
    """
    store = get_feedback_store()
    await store.create_feedback(interaction_id, thumb, comment)

    if thumb == "DOWN":
        await store.ensure_review_queue(interaction_id)

    return "OK"

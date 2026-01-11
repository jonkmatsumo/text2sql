from typing import Optional

from mcp_server.config.database import Database
from mcp_server.dal.feedback import FeedbackDAL


async def submit_feedback_tool(
    interaction_id: str, thumb: str, comment: Optional[str] = None
) -> str:
    """
    Submit user feedback (UP/DOWN) for a specific interaction.

    Args:
        interaction_id: The UUID of the interaction being rated.
        thumb: "UP" or "DOWN".
        comment: Optional text feedback.

    Returns: "OK"
    """
    async with Database.get_connection() as conn:
        dal = FeedbackDAL(conn)
        await dal.create_feedback(interaction_id, thumb, comment)

        if thumb == "DOWN":
            await dal.ensure_review_queue(interaction_id)

    return "OK"

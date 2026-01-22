"""MCP tool: submit_feedback - Submit user feedback for an interaction."""

import logging
from typing import Optional

from dal.factory import get_feedback_store

TOOL_NAME = "submit_feedback"

logger = logging.getLogger(__name__)


class FeedbackLinkageError(Exception):
    """Raised when feedback linkage to interaction fails."""

    pass


async def handler(interaction_id: str, thumb: str, comment: Optional[str] = None) -> str:
    """Submit user feedback (UP/DOWN) for a specific interaction.

    Args:
        interaction_id: The unique identifier of the interaction.
        thumb: Feedback type - "UP" for positive, "DOWN" for negative.
        comment: Optional comment with the feedback.

    Returns:
        "OK" on success.

    Raises:
        FeedbackLinkageError: If interaction_id is None/empty or FK constraint fails.
    """
    # Validate interaction_id before attempting write
    if not interaction_id or not interaction_id.strip():
        logger.error(
            "Feedback submission failed: interaction_id is required",
            extra={
                "operation": "submit_feedback",
                "thumb": thumb,
                "interaction_id": interaction_id,
            },
        )
        raise FeedbackLinkageError("interaction_id is required for feedback submission")

    store = get_feedback_store()

    try:
        await store.create_feedback(interaction_id, thumb, comment)

        if thumb == "DOWN":
            await store.ensure_review_queue(interaction_id)

        return "OK"
    except Exception as e:
        # Check for FK violation patterns
        error_msg = str(e).lower()
        if (
            "foreign key" in error_msg
            or "violates foreign key" in error_msg
            or "fk_" in error_msg
            or "references" in error_msg
        ):
            logger.error(
                "Feedback FK violation: interaction does not exist",
                extra={
                    "operation": "submit_feedback",
                    "interaction_id": interaction_id,
                    "thumb": thumb,
                    "exception_type": type(e).__name__,
                },
                exc_info=True,
            )
            raise FeedbackLinkageError(f"Interaction '{interaction_id}' does not exist: {e}") from e

        # Re-raise other errors with context
        logger.error(
            "Feedback submission failed",
            extra={
                "operation": "submit_feedback",
                "interaction_id": interaction_id,
                "thumb": thumb,
                "exception_type": type(e).__name__,
            },
            exc_info=True,
        )
        raise

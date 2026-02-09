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
    """Submit user feedback (UP/DOWN) for a specific interaction."""
    from mcp_server.utils.envelopes import tool_error_response, tool_success_response

    # Validate interaction_id before attempting write
    if not interaction_id or not interaction_id.strip():
        return tool_error_response("interaction_id is required", category="invalid_request")

    store = get_feedback_store()

    try:
        await store.create_feedback(interaction_id, thumb, comment)

        if thumb == "DOWN":
            await store.ensure_review_queue(interaction_id)

        return tool_success_response("OK")
    except Exception as e:
        # Check for FK violation patterns
        error_msg = str(e).lower()
        if (
            "foreign key" in error_msg
            or "violates foreign key" in error_msg
            or "fk_" in error_msg
            or "references" in error_msg
        ):
            return tool_error_response(
                f"Interaction '{interaction_id}' does not exist", category="invalid_request"
            )

        # Re-raise other errors or return standardized error
        return tool_error_response(
            f"Failed to submit feedback: {str(e)}", category="internal_error"
        )

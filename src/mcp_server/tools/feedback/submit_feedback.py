"""MCP tool: submit_feedback - Submit user feedback for an interaction."""

from typing import Optional

from common.models.error_metadata import ErrorCategory
from dal.factory import get_feedback_store
from mcp_server.utils.envelopes import tool_success_response
from mcp_server.utils.errors import tool_error_response


class FeedbackLinkageError(Exception):
    """Raised when feedback cannot be linked to an interaction."""

    pass


TOOL_NAME = "submit_feedback"
TOOL_DESCRIPTION = "Submit user feedback (UP/DOWN) for a specific interaction."


async def handler(
    interaction_id: str,
    thumb: str,
    comment: Optional[str] = None,
    tenant_id: Optional[int] = None,
) -> str:
    """Submit user feedback (UP/DOWN) for a specific interaction.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Write access to the feedback store. Feedback is linked by interaction_id.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Invalid Request: If interaction_id is missing or doesn't exist.
        - Database Error: If the feedback store is unavailable.

    Args:
        interaction_id: The unique identifier of the interaction.
        thumb: UP or DOWN.
        comment: Optional textual feedback.
        tenant_id: Tenant identifier.

    Returns:
        JSON string with success or error status.
    """
    from mcp_server.utils.auth import validate_role
    from mcp_server.utils.validation import require_tenant_id

    if err := validate_role("SQL_USER_ROLE", TOOL_NAME):
        return err

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    # Validate interaction_id before attempting write
    if not interaction_id or not interaction_id.strip():
        return tool_error_response(
            message="interaction_id is required",
            code="INVALID_INTERACTION_ID",
            category="invalid_request",
        )

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
                message=f"Interaction '{interaction_id}' does not exist",
                code="INTERACTION_NOT_FOUND",
                category="invalid_request",
            )

        # Re-raise other errors or return standardized error
        return tool_error_response(
            message="Failed to submit feedback.",
            code="SUBMIT_FEEDBACK_FAILED",
            category=ErrorCategory.INTERNAL,
        )

"""MCP tool: save_conversation_state - Save conversation state to persistent storage."""

from typing import Any, Dict

from dal.factory import get_conversation_store

TOOL_NAME = "save_conversation_state"
TOOL_DESCRIPTION = "Save conversation state to persistent storage."


async def handler(
    conversation_id: str,
    user_id: str,
    state_json: Dict[str, Any],
    version: int,
    ttl_minutes: int = 60,
) -> str:
    """Save the conversation state to persistent storage.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Write access to the conversation state store. Scoped by conversation_id and user_id.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Validation Error: If the state_json or version is malformed.
        - Database Error: If the conversation store is unavailable.

    Args:
        conversation_id: Unique identifier for the conversation.
        user_id: User identifier.
        state_json: Dictionary containing the state to persist.
        version: Version number of the state.
        ttl_minutes: Time-to-live for the cached state (default: 60).

    Returns:
        JSON-encoded ToolResponseEnvelope with "OK" status.
    """
    from mcp_server.utils.envelopes import tool_success_response

    store = get_conversation_store()
    await store.save_state_async(conversation_id, user_id, state_json, version, ttl_minutes)
    return tool_success_response("OK")

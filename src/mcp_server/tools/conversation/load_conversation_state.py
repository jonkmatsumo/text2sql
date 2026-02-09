"""MCP tool: load_conversation_state - Load conversation state from persistent storage."""

from dal.factory import get_conversation_store

TOOL_NAME = "load_conversation_state"
TOOL_DESCRIPTION = "Load conversation state from persistent storage."


async def handler(conversation_id: str, user_id: str) -> str:
    """Load the conversation state from persistent storage.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher).

    Data Access:
        Read-only access to the conversation state store. Scoped by conversation_id and user_id.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Not Found: Returns an empty state if no state exists for the given IDs.
        - Database Error: If the conversation store is unavailable.

    Args:
        conversation_id: Unique identifier for the conversation.
        user_id: User identifier.

    Returns:
        JSON-encoded ToolResponseEnvelope containing the state JSON dictionary.
    """
    from mcp_server.utils.envelopes import tool_success_response

    store = get_conversation_store()
    state = await store.load_state_async(conversation_id, user_id)
    return tool_success_response(state)

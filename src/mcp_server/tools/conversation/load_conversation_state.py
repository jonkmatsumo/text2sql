"""MCP tool: load_conversation_state - Load conversation state from persistent storage."""

from dal.factory import get_conversation_store

TOOL_NAME = "load_conversation_state"


async def handler(conversation_id: str, user_id: str) -> str:
    """Load the conversation state from persistent storage.

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

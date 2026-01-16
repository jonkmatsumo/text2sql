"""MCP tool: load_conversation_state - Load conversation state from persistent storage."""

from typing import Any, Dict, Optional

from dal.factory import get_conversation_store

TOOL_NAME = "load_conversation_state"


async def handler(conversation_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Load the conversation state from persistent storage.

    Args:
        conversation_id: Unique identifier for the conversation.
        user_id: User identifier.

    Returns:
        The state JSON dictionary or None if not found/expired.
    """
    store = get_conversation_store()
    return await store.load_state_async(conversation_id, user_id)

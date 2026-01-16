"""MCP tool: save_conversation_state - Save conversation state to persistent storage."""

from typing import Any, Dict

from dal.factory import get_conversation_store

TOOL_NAME = "save_conversation_state"


async def handler(
    conversation_id: str,
    user_id: str,
    state_json: Dict[str, Any],
    version: int,
    ttl_minutes: int = 60,
) -> str:
    """Save the conversation state to persistent storage.

    Args:
        conversation_id: Unique identifier for the conversation.
        user_id: User identifier.
        state_json: Arbitrary state dictionary to persist.
        version: Version number for optimistic locking.
        ttl_minutes: Time-to-live in minutes (default: 60).

    Returns:
        "OK" on success.
    """
    store = get_conversation_store()
    await store.save_state_async(conversation_id, user_id, state_json, version, ttl_minutes)
    return "OK"

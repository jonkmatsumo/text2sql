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
    """Save the conversation state to persistent storage."""
    from mcp_server.utils.envelopes import tool_success_response

    store = get_conversation_store()
    await store.save_state_async(conversation_id, user_id, state_json, version, ttl_minutes)
    return tool_success_response("OK")

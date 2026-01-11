from typing import Any, Dict, Optional

from mcp_server.config.database import Database
from mcp_server.dal.conversation import ConversationDAL


async def save_conversation_state(
    conversation_id: str,
    user_id: str,
    state_json: Dict[str, Any],
    version: int,
    ttl_minutes: int = 60,
) -> str:
    """
    Save the conversation state to persistent storage.

    Args:
        conversation_id: Unique identifier for the conversation
        user_id: The owner of the conversation
        state_json: The full state object as a JSON-compatible dictionary
        version: The version number of the state (for optimistic locking)
        ttl_minutes: How long the state should live before cleanup (default 60)

    Returns:
        "OK" on success
    """
    async with Database.get_connection() as conn:
        # Wrap asyncpg connection in sync-looking DAL?
        # Wait, my DAL was designed for sync `execute` calls in the test mock.
        # But asyncpg is async. I need to make the DAL async or handle it here.
        # The `Database.get_connection()` returns an async connection.

        # Refactoring DAL to be async-aware is correct for asyncpg.
        # Let's assume I fix the DAL to use `await conn.execute(...)`.

        dal = ConversationDAL(conn)
        await dal.save_state_async(conversation_id, user_id, state_json, version, ttl_minutes)
    return "OK"


async def load_conversation_state(conversation_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Load the conversation state from persistent storage.

    Returns:
        The state JSON dictionary or None if not found/expired.
    """
    async with Database.get_connection() as conn:
        dal = ConversationDAL(conn)
        return await dal.load_state_async(conversation_id, user_id)

"""MCP tool: list_interactions - List recent query interactions."""

from typing import List

from mcp_server.dal.factory import get_interaction_store

TOOL_NAME = "list_interactions"


async def handler(limit: int = 50, offset: int = 0) -> List[dict]:
    """List recent query interactions with feedback summary.

    Args:
        limit: Maximum number of interactions to return (default: 50).
        offset: Number of interactions to skip (default: 0).

    Returns:
        List of interaction dictionaries.
    """
    store = get_interaction_store()
    return await store.get_recent_interactions(limit, offset)

"""MCP tool: list_interactions - List recent query interactions."""

from dal.factory import get_interaction_store

TOOL_NAME = "list_interactions"


async def handler(limit: int = 50, offset: int = 0) -> str:
    """List recent query interactions with feedback summary.

    Args:
        limit: Maximum number of interactions to return (default: 50).
        offset: Number of interactions to skip (default: 0).

    Returns:
        List of interaction dictionaries.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.validation import validate_limit

    if err := validate_limit(limit, TOOL_NAME):
        return err

    start_time = time.monotonic()

    store = get_interaction_store()
    results = await store.get_recent_interactions(limit, offset)

    execution_time_ms = (time.monotonic() - start_time) * 1000

    return ToolResponseEnvelope(
        result=results,
        metadata=GenericToolMetadata(
            provider="interaction_store", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

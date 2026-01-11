"""MCP tool: get_interaction_details - Get full details for an interaction."""

from mcp_server.dal.factory import get_feedback_store, get_interaction_store

TOOL_NAME = "get_interaction_details"


async def handler(interaction_id: str) -> dict:
    """Get full details for a specific interaction, including all feedback.

    Args:
        interaction_id: The unique identifier of the interaction.

    Returns:
        Dictionary with interaction details and feedback list.
    """
    i_store = get_interaction_store()
    f_store = get_feedback_store()

    interaction = await i_store.get_interaction_detail(interaction_id)
    if not interaction:
        return {"error": f"Interaction {interaction_id} not found"}

    feedback = await f_store.get_feedback_for_interaction(interaction_id)
    interaction["feedback"] = feedback
    return interaction

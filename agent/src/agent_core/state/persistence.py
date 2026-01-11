import json
from typing import Any, Optional

from agent_core.state.domain import ConversationState


class PersistenceAdapter:
    """Client-side adapter to persist ConversationState via MCP tools."""

    def __init__(self, mcp_client: Any):
        """Initialize adapter with MCP client."""
        self.client = mcp_client

    async def save_state_async(
        self, state: ConversationState, user_id: str, ttl_minutes: int = 60
    ) -> None:
        """Serialize and save state via MCP."""
        # Convert state to dict (using internal method or asdict)
        # Assuming we can just use json-compatible dict
        # The domain object has `to_json`, but tool args expect a dict usually?
        # Let's inspect `save_conversation_state` signature in tools.
        # It expects `state_json: Dict[str, Any]`.
        # So we can use `json.loads(state.to_json())` or implement `to_dict`.

        state_str = state.to_json()
        state_dict = json.loads(state_str)

        await self.client.call_tool(
            "save_conversation_state",
            {
                "conversation_id": state.conversation_id,
                "user_id": user_id,
                "state_json": state_dict,
                "version": state.state_version,
                "ttl_minutes": ttl_minutes,
            },
        )

    async def load_state_async(
        self, conversation_id: str, user_id: str
    ) -> Optional[ConversationState]:
        """Load and deserialize state via MCP."""
        result = await self.client.call_tool(
            "load_conversation_state", {"conversation_id": conversation_id, "user_id": user_id}
        )

        if not result:
            return None

        # Result is Dict from MCP
        return ConversationState.from_json(json.dumps(result))

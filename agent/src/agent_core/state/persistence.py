import json
from typing import Any, List, Optional

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


class InteractionAdapter:
    """Client-side adapter to log query interactions via MCP tools."""

    def __init__(self, mcp_client: Any):
        """Initialize adapter with MCP client."""
        self.client = mcp_client

    async def create_interaction_async(
        self,
        conversation_id: Optional[str],
        schema_snapshot_id: str,
        user_nlq_text: str,
        model_version: Optional[str] = None,
        prompt_version: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """Call MCP to log start of interaction."""
        result = await self.client.call_tool(
            "create_interaction",
            {
                "conversation_id": conversation_id,
                "schema_snapshot_id": schema_snapshot_id,
                "user_nlq_text": user_nlq_text,
                "model_version": model_version,
                "prompt_version": prompt_version,
                "trace_id": trace_id,
            },
        )
        # FastMCP usually returns the result of the function
        return str(result)

    async def update_interaction_async(
        self,
        interaction_id: str,
        generated_sql: Optional[str] = None,
        response_payload: Optional[Any] = None,
        execution_status: str = "SUCCESS",
        error_type: Optional[str] = None,
        tables_used: Optional[List[str]] = None,
    ) -> None:
        """Call MCP to log result of interaction."""
        # Payload should be string for the tool if we want to be safe,
        # but the tool handles dict->json.
        await self.client.call_tool(
            "update_interaction",
            {
                "interaction_id": interaction_id,
                "generated_sql": generated_sql,
                "response_payload": json.dumps(response_payload) if response_payload else None,
                "execution_status": execution_status,
                "error_type": error_type,
                "tables_used": tables_used,
            },
        )


class FeedbackAdapter:
    """Client-side adapter to submit feedback via MCP tools."""

    def __init__(self, mcp_client: Any):
        """Initialize adapter with MCP client."""
        self.client = mcp_client

    async def submit_feedback_async(
        self, interaction_id: str, thumb: str, comment: Optional[str] = None
    ) -> None:
        """Submit feedback to MCP."""
        await self.client.call_tool(
            "submit_feedback",
            {"interaction_id": interaction_id, "thumb": thumb, "comment": comment},
        )

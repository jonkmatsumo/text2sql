from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class ConversationStore(Protocol):
    """Protocol for conversation state persistence."""

    async def save_state_async(
        self,
        conversation_id: str,
        user_id: str,
        tenant_id: int,
        state_json: Dict[str, Any],
        version: int,
        ttl_minutes: int = 60,
    ) -> None:
        """Upsert conversation state."""
        ...

    async def load_state_async(
        self, conversation_id: str, user_id: str, tenant_id: int
    ) -> Optional[Dict[str, Any]]:
        """Load conversation state."""
        ...

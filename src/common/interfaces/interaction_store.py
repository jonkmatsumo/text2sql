from typing import Any, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class InteractionStore(Protocol):
    """Protocol for query interaction persistence."""

    async def create_interaction(
        self,
        conversation_id: Optional[str],
        schema_snapshot_id: str,
        user_nlq_text: str,
        tenant_id: int = 1,
        model_version: Optional[str] = None,
        prompt_version: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """Create a new interaction record and return its UUID."""
        ...

    async def update_interaction_result(
        self,
        interaction_id: str,
        generated_sql: Optional[str] = None,
        response_payload: Optional[Any] = None,
        execution_status: str = "SUCCESS",
        error_type: Optional[str] = None,
        tables_used: Optional[List[str]] = None,
    ) -> None:
        """Update an interaction with execution results."""
        ...

    async def get_recent_interactions(self, limit: int = 50, offset: int = 0) -> List[dict]:
        """Fetch list of user interactions with feedback summary.

        Returns:
            List of dicts containing: id, conversation_id, user_nlq_text,
            execution_status, created_at, trace_id, generated_sql_preview, thumb.
        """
        ...

    async def get_interaction_detail(self, interaction_id: str) -> Optional[dict]:
        """Fetch full details for a single interaction."""
        ...

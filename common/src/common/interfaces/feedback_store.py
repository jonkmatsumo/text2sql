from typing import List, Optional, Protocol, runtime_checkable


@runtime_checkable
class FeedbackStore(Protocol):
    """Protocol for feedback and review queue access."""

    async def create_feedback(
        self,
        interaction_id: str,
        thumb: str,
        comment: Optional[str] = None,
        feedback_source: str = "end_user",
    ) -> str:
        """Create feedback row and return its ID."""
        ...

    async def ensure_review_queue(self, interaction_id: str) -> None:
        """Ensure a PENDING review item exists for this interaction."""
        ...

    async def get_feedback_for_interaction(self, interaction_id: str) -> List[dict]:
        """Fetch all feedback rows for an interaction."""
        ...

    async def update_review_status(
        self,
        interaction_id: str,
        status: str,
        resolution_type: Optional[str] = None,
        corrected_sql: Optional[str] = None,
        reviewer_notes: Optional[str] = None,
    ) -> None:
        """Update review queue status and resolution."""
        ...

    async def get_approved_interactions(self, limit: int = 50) -> List[dict]:
        """Fetch interactions that are APPROVED but not yet PUBLISHED."""
        ...

    async def set_published_status(self, interaction_id: str) -> None:
        """Mark a review item as PUBLISHED."""
        ...

from typing import Any, Optional


class FeedbackDAL:
    """Data Access Layer for Feedback and Review Queue."""

    def __init__(self, db_client: Any):
        """Initialize with DB client."""
        self.db = db_client

    async def create_feedback(
        self,
        interaction_id: str,
        thumb: str,
        comment: Optional[str] = None,
        feedback_source: str = "end_user",
    ) -> str:
        """Create feedback row."""
        sql = """
            INSERT INTO feedback (
                interaction_id, thumb, comment, feedback_source, created_at
            ) VALUES (
                $1::uuid, $2, $3, $4, NOW()
            )
            RETURNING id::text
        """
        return await self.db.fetchval(sql, interaction_id, thumb, comment, feedback_source)

    async def ensure_review_queue(self, interaction_id: str) -> None:
        """
        Ensure a PENDING review item exists for this interaction.

        Idempotent via ON CONFLICT DO NOTHING (if unique constraint exists)
        or WHERE NOT EXISTS check.
        Requires unique index on (interaction_id) WHERE status='PENDING'.
        """
        sql = """
            INSERT INTO review_queue (
                interaction_id, status, created_at, updated_at
            ) VALUES (
                $1::uuid, 'PENDING', NOW(), NOW()
            )
            ON CONFLICT (interaction_id) WHERE status = 'PENDING'
            DO NOTHING
        """
        await self.db.execute(sql, interaction_id)

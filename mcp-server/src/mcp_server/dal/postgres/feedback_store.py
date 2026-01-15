from contextlib import asynccontextmanager
from typing import Any, List, Optional

from mcp_server.config.control_plane import ControlPlaneDatabase

from common.interfaces.feedback_store import FeedbackStore


class PostgresFeedbackStore(FeedbackStore):
    """PostgreSQL implementation of feedback and review queue DAL."""

    def __init__(self, db_client: Any = None):
        """Initialize with optional DB client for testing."""
        self.db = db_client

    @asynccontextmanager
    async def _get_connection(self):
        """Get connection from injected client or the pool."""
        if self.db:
            yield self.db
        else:
            async with ControlPlaneDatabase.get_connection() as conn:
                yield conn

    async def create_feedback(
        self,
        interaction_id: str,
        thumb: str,
        comment: Optional[str] = None,
        feedback_source: str = "end_user",
    ) -> str:
        """Create feedback row and return ID."""
        sql = """
            INSERT INTO feedback (
                interaction_id, thumb, comment, feedback_source, created_at
            ) VALUES (
                $1::uuid, $2, $3, $4, NOW()
            )
            RETURNING id::text
        """
        async with self._get_connection() as conn:
            return await conn.fetchval(sql, interaction_id, thumb, comment, feedback_source)

    async def ensure_review_queue(self, interaction_id: str) -> None:
        """Ensure a PENDING review item exists."""
        sql = """
            INSERT INTO review_queue (
                interaction_id, status, created_at, updated_at
            ) VALUES (
                $1::uuid, 'PENDING', NOW(), NOW()
            )
            ON CONFLICT (interaction_id) WHERE status = 'PENDING'
            DO NOTHING
        """
        async with self._get_connection() as conn:
            await conn.execute(sql, interaction_id)

    async def get_feedback_for_interaction(self, interaction_id: str) -> List[dict]:
        """Fetch feedback rows for an interaction."""
        sql = """
            SELECT id::text, thumb, comment, feedback_source, created_at
            FROM feedback
            WHERE interaction_id = $1::uuid
            ORDER BY created_at DESC
        """
        async with self._get_connection() as conn:
            rows = await conn.fetch(sql, interaction_id)
        return [dict(r) for r in rows]

    async def update_review_status(
        self,
        interaction_id: str,
        status: str,
        resolution_type: Optional[str] = None,
        corrected_sql: Optional[str] = None,
        reviewer_notes: Optional[str] = None,
    ) -> None:
        """Update review queue status."""
        sql = """
            UPDATE review_queue
            SET
                status = $2,
                resolution_type = $3,
                corrected_sql = $4,
                reviewer_notes = $5,
                updated_at = NOW()
            WHERE interaction_id = $1::uuid AND status = 'PENDING'
        """
        async with self._get_connection() as conn:
            await conn.execute(
                sql, interaction_id, status, resolution_type, corrected_sql, reviewer_notes
            )

    async def get_approved_interactions(self, limit: int = 50) -> List[dict]:
        """Fetch interactions that are APPROVED."""
        sql = """
            SELECT
                rq.interaction_id::text,
                rq.corrected_sql,
                rq.resolution_type,
                qi.user_nlq_text,
                qi.conversation_id,
                qi.tenant_id
            FROM review_queue rq
            JOIN query_interactions qi ON rq.interaction_id = qi.id
            WHERE rq.status = 'APPROVED'
            LIMIT $1
        """
        async with self._get_connection() as conn:
            rows = await conn.fetch(sql, limit)
        return [dict(r) for r in rows]

    async def set_published_status(self, interaction_id: str) -> None:
        """Mark as PUBLISHED."""
        sql = """
            UPDATE review_queue
            SET status = 'PUBLISHED', updated_at = NOW()
            WHERE interaction_id = $1::uuid
        """
        async with self._get_connection() as conn:
            await conn.execute(sql, interaction_id)

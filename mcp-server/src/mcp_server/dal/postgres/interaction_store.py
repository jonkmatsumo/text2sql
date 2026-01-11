import json
from contextlib import asynccontextmanager
from typing import Any, List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.interaction_store import InteractionStore


class PostgresInteractionStore(InteractionStore):
    """PostgreSQL implementation of query interaction DAL."""

    def __init__(self, db_client: Any = None):
        """Initialize with optional DB client for testing."""
        self.db = db_client

    @asynccontextmanager
    async def _get_connection(self):
        """Get connection from injected client or the pool."""
        if self.db:
            yield self.db
        else:
            async with Database.get_connection() as conn:
                yield conn

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
        """Create a new interaction record."""
        sql = """
            INSERT INTO query_interactions (
                conversation_id, schema_snapshot_id, user_nlq_text,
                tenant_id, model_version, prompt_version, trace_id, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, NOW()
            )
            RETURNING id::text
        """
        async with self._get_connection() as conn:
            return await conn.fetchval(
                sql,
                conversation_id,
                schema_snapshot_id,
                user_nlq_text,
                tenant_id,
                model_version,
                prompt_version,
                trace_id,
            )

    async def update_interaction_result(
        self,
        interaction_id: str,
        generated_sql: Optional[str] = None,
        response_payload: Optional[Any] = None,
        execution_status: str = "SUCCESS",
        error_type: Optional[str] = None,
        tables_used: Optional[List[str]] = None,
    ) -> None:
        """Update an interaction with results."""
        payload_json = response_payload
        if isinstance(payload_json, (dict, list)):
            payload_json = json.dumps(payload_json)

        sql = """
            UPDATE query_interactions
            SET
                generated_sql = $2,
                response_payload = $3::jsonb,
                execution_status = $4,
                error_type = $5,
                tables_used = $6
            WHERE id = $1::uuid
        """
        async with self._get_connection() as conn:
            await conn.execute(
                sql,
                interaction_id,
                generated_sql,
                payload_json,
                execution_status,
                error_type,
                tables_used,
            )

    async def get_recent_interactions(self, limit: int = 50, offset: int = 0) -> List[dict]:
        """Fetch list of user interactions."""
        sql = """
            SELECT
                i.id::text,
                i.conversation_id,
                i.user_nlq_text,
                i.execution_status,
                i.created_at,
                f.thumb
            FROM query_interactions i
            LEFT JOIN (
                SELECT DISTINCT ON (interaction_id) interaction_id, thumb, created_at
                FROM feedback
                ORDER BY interaction_id, created_at DESC
            ) f ON i.id = f.interaction_id
            ORDER BY i.created_at DESC
            LIMIT $1 OFFSET $2
        """
        async with self._get_connection() as conn:
            rows = await conn.fetch(sql, limit, offset)
        return [dict(r) for r in rows]

    async def get_interaction_detail(self, interaction_id: str) -> Optional[dict]:
        """Fetch full details for an interaction."""
        sql = """
            SELECT
                id::text,
                conversation_id,
                schema_snapshot_id,
                user_nlq_text,
                generated_sql,
                response_payload,
                execution_status,
                error_type,
                tables_used,
                model_version,
                prompt_version,
                trace_id,
                created_at
            FROM query_interactions
            WHERE id = $1::uuid
        """
        async with self._get_connection() as conn:
            row = await conn.fetchrow(sql, interaction_id)
        return dict(row) if row else None

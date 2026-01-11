import json
from typing import Any, List, Optional


class InteractionDAL:
    """Data Access Layer for Query Interactions."""

    def __init__(self, db_client: Any):
        """Initialize with DB client (asyncpg pool or connection)."""
        self.db = db_client

    async def create_interaction(
        self,
        conversation_id: Optional[str],
        schema_snapshot_id: str,
        user_nlq_text: str,
        model_version: Optional[str] = None,
        prompt_version: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> str:
        """
        Create a new interaction record at start of request.

        Returns the generated UUID as string.
        """
        sql = """
            INSERT INTO query_interactions (
                conversation_id, schema_snapshot_id, user_nlq_text,
                model_version, prompt_version, trace_id, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, NOW()
            )
            RETURNING id::text
        """
        return await self.db.fetchval(
            sql,
            conversation_id,
            schema_snapshot_id,
            user_nlq_text,
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
        """Update an interaction with execution results."""
        # Convert dict payload to JSON string if needed
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
        await self.db.execute(
            sql,
            interaction_id,
            generated_sql,
            payload_json,
            execution_status,
            error_type,
            tables_used,
        )

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from common.interfaces.conversation_store import ConversationStore
from dal.control_plane import ControlPlaneDatabase


class PostgresConversationStore(ConversationStore):
    """PostgreSQL implementation of conversation state persistence."""

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

    async def save_state_async(
        self,
        conversation_id: str,
        user_id: str,
        state_json: Dict[str, Any],
        version: int,
        ttl_minutes: int = 60,
    ) -> None:
        """Upsert conversation state asynchronously."""
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
        state_str = json.dumps(state_json)

        sql = """
            INSERT INTO conversation_states (
                conversation_id, user_id, state_version, state_json, updated_at, expires_at
            ) VALUES (
                $1, $2, $3, $4, NOW(), $5
            )
            ON CONFLICT (conversation_id) DO UPDATE SET
                state_version = EXCLUDED.state_version,
                state_json = EXCLUDED.state_json,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
        """
        async with self._get_connection() as conn:
            await conn.execute(sql, conversation_id, user_id, version, state_str, expires_at)

    async def load_state_async(
        self, conversation_id: str, user_id: str
    ) -> Optional[Dict[str, Any]]:
        """Load state if exists and not expired."""
        sql = """
            SELECT state_json FROM conversation_states
            WHERE conversation_id = $1
              AND user_id = $2
              AND expires_at > NOW()
        """
        async with self._get_connection() as conn:
            result = await conn.fetchrow(sql, conversation_id, user_id)

        if result and result.get("state_json"):
            raw = result["state_json"]
            if isinstance(raw, str):
                return json.loads(raw)
            return raw
        return None

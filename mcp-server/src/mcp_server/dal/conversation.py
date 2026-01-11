import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


class ConversationDAL:
    """Data Access Layer for Conversation States."""

    def __init__(self, db_client: Any):
        """Initialize with DB client."""
        self.db = db_client

    def save_state(
        self,
        conversation_id: str,
        user_id: str,
        state_json: Dict[str, Any],
        version: int,
        ttl_minutes: int = 60,
    ) -> None:
        """Upsert conversation state."""
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
        # Using json.dumps ensures it's a string for SQL JSONB
        state_str = json.dumps(state_json)

        sql = """
            INSERT INTO conversation_states (
                conversation_id, user_id, state_version, state_json, updated_at, expires_at
            ) VALUES (
                :conversation_id, :user_id, :version, :state_json, NOW(), :expires_at
            )
            ON CONFLICT (conversation_id) DO UPDATE SET
                state_version = EXCLUDED.state_version,
                state_json = EXCLUDED.state_json,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
        """

        self.db.execute(
            sql,
            {
                "conversation_id": conversation_id,
                "user_id": user_id,
                "version": version,
                "state_json": state_str,
                "expires_at": expires_at,
            },
        )

    def load_state(self, conversation_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Load state if exists and not active."""
        sql = """
            SELECT state_json FROM conversation_states
            WHERE conversation_id = :conversation_id
              AND user_id = :user_id
              AND expires_at > NOW()
        """

        result = self.db.fetch_one(sql, {"conversation_id": conversation_id, "user_id": user_id})

        if result and result.get("state_json"):
            # Depending on DB driver, state_json might be object or string
            raw = result["state_json"]
            if isinstance(raw, str):
                return json.loads(raw)
            return raw
        return None

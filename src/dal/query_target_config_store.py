import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

import asyncpg

from common.config.env import get_env_int, get_env_str
from dal.query_target_config import (
    QueryTargetConfigHistoryRecord,
    QueryTargetConfigRecord,
    QueryTargetConfigStatus,
)

logger = logging.getLogger(__name__)


class QueryTargetConfigStore:
    """Store for persisted query-target settings (control-plane)."""

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(cls) -> bool:
        """Initialize pool for control-plane access."""
        if cls._pool is not None:
            return True

        db_host = get_env_str("CONTROL_DB_HOST")
        if not db_host:
            logger.warning("CONTROL_DB_HOST not set, query-target settings disabled")
            return False

        db_port = get_env_int("CONTROL_DB_PORT", 5432)
        db_name = get_env_str("CONTROL_DB_NAME", "agent_control")
        db_user = get_env_str("CONTROL_DB_USER", "postgres")
        db_pass = get_env_str("CONTROL_DB_PASSWORD", "control_password")

        dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

        try:
            cls._pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=5,
                command_timeout=30,
                server_settings={"application_name": "query_target_config_store"},
            )
            logger.info("QueryTargetConfigStore connected to control-plane DB")
            return True
        except Exception as exc:
            logger.error("Failed to connect to control-plane DB: %s", exc)
            return False

    @classmethod
    async def close(cls) -> None:
        """Close the connection pool."""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            logger.info("QueryTargetConfigStore connection pool closed")

    @classmethod
    def is_available(cls) -> bool:
        """Return whether the store is available."""
        return cls._pool is not None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Yield a connection from the pool."""
        if cls._pool is None:
            raise RuntimeError(
                "QueryTargetConfigStore not initialized. Call QueryTargetConfigStore.init() first."
            )

        async with cls._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    @classmethod
    async def upsert_config(
        cls,
        provider: str,
        metadata: Dict[str, Any],
        auth: Dict[str, Any],
        guardrails: Dict[str, Any],
        status: QueryTargetConfigStatus = QueryTargetConfigStatus.INACTIVE,
    ) -> QueryTargetConfigRecord:
        """Create or update a query-target config by provider."""
        payload = {
            "provider": provider,
            "metadata": json.dumps(metadata),
            "auth": json.dumps(auth),
            "guardrails": json.dumps(guardrails),
            "status": status.value,
        }
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO query_target_configs (provider, metadata, auth, guardrails, status)
                VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5)
                ON CONFLICT (provider)
                DO UPDATE SET
                    metadata = EXCLUDED.metadata,
                    auth = EXCLUDED.auth,
                    guardrails = EXCLUDED.guardrails,
                    status = EXCLUDED.status
                RETURNING *, (xmax = 0) AS inserted
                """,
                payload["provider"],
                payload["metadata"],
                payload["auth"],
                payload["guardrails"],
                payload["status"],
            )
            if row:
                try:
                    inserted = row["inserted"]
                except Exception:
                    inserted = False
                event_type = "created" if inserted else "updated"
                await _record_history(conn, row["id"], event_type, _row_to_snapshot(row))
        return _row_to_record(row)

    @classmethod
    async def get_active(cls) -> Optional[QueryTargetConfigRecord]:
        """Fetch the active query-target config, if any."""
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM query_target_configs WHERE status = $1",
                QueryTargetConfigStatus.ACTIVE.value,
            )
        return _row_to_record(row) if row else None

    @classmethod
    async def get_pending(cls) -> Optional[QueryTargetConfigRecord]:
        """Fetch the pending query-target config, if any."""
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM query_target_configs WHERE status = $1",
                QueryTargetConfigStatus.PENDING.value,
            )
        return _row_to_record(row) if row else None

    @classmethod
    async def get_by_id(cls, config_id: UUID) -> Optional[QueryTargetConfigRecord]:
        """Fetch a query-target config by id."""
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM query_target_configs WHERE id = $1",
                config_id,
            )
        return _row_to_record(row) if row else None

    @classmethod
    async def set_pending(cls, config_id: UUID) -> None:
        """Mark a config as pending and clear existing pending statuses."""
        async with cls.get_connection() as conn:
            await conn.execute(
                """
                UPDATE query_target_configs
                SET status = $2,
                    deactivated_at = NOW()
                WHERE status = $3
                  AND id <> $1
                """,
                config_id,
                QueryTargetConfigStatus.INACTIVE.value,
                QueryTargetConfigStatus.PENDING.value,
            )
            await conn.execute(
                """
                UPDATE query_target_configs
                SET status = $2
                WHERE id = $1
                """,
                config_id,
                QueryTargetConfigStatus.PENDING.value,
            )

    @classmethod
    async def set_status(
        cls,
        config_id: UUID,
        status: QueryTargetConfigStatus,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        activated: bool = False,
        deactivated: bool = False,
    ) -> None:
        """Update status fields for a config."""
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                """
                UPDATE query_target_configs
                SET status = $2,
                    last_error_code = $3,
                    last_error_message = $4,
                    activated_at = CASE WHEN $5 THEN NOW() ELSE activated_at END,
                    deactivated_at = CASE WHEN $6 THEN NOW() ELSE deactivated_at END
                WHERE id = $1
                """,
                config_id,
                status.value,
                error_code,
                error_message,
                activated,
                deactivated,
            )
            if not row:
                return
            snapshot = _row_to_snapshot(row)
            if status == QueryTargetConfigStatus.UNHEALTHY:
                await _record_history(conn, config_id, "unhealthy", snapshot)
            if activated:
                await _record_history(conn, config_id, "activated", snapshot)
            if deactivated:
                await _record_history(conn, config_id, "deactivated", snapshot)

    @classmethod
    async def record_test_result(
        cls,
        config_id: UUID,
        status: str,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Record last test-connection result."""
        async with cls.get_connection() as conn:
            row = await conn.fetchrow(
                """
                UPDATE query_target_configs
                SET last_tested_at = NOW(),
                    last_test_status = $2,
                    last_error_code = $3,
                    last_error_message = $4
                WHERE id = $1
                RETURNING *
                """,
                config_id,
                status,
                error_code,
                error_message,
            )
            if row:
                await _record_history(conn, config_id, "tested", _row_to_snapshot(row))

    @classmethod
    async def list_history(cls, limit: int = 50) -> list[QueryTargetConfigHistoryRecord]:
        """List recent query-target configuration history entries."""
        async with cls.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT *
                FROM query_target_config_history
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [_history_row_to_record(row) for row in rows]


def _row_to_record(row: asyncpg.Record) -> QueryTargetConfigRecord:
    return QueryTargetConfigRecord(
        id=row["id"],
        provider=row["provider"],
        metadata=row["metadata"] or {},
        auth=row["auth"] or {},
        guardrails=row["guardrails"] or {},
        status=QueryTargetConfigStatus(row["status"]),
        last_tested_at=row["last_tested_at"].isoformat() if row["last_tested_at"] else None,
        last_test_status=row["last_test_status"],
        last_error_code=row["last_error_code"],
        last_error_message=row["last_error_message"],
    )


def _history_row_to_record(row: asyncpg.Record) -> QueryTargetConfigHistoryRecord:
    created_at = row["created_at"].isoformat() if row["created_at"] else None
    return QueryTargetConfigHistoryRecord(
        id=row["id"],
        config_id=row["config_id"],
        event_type=row["event_type"],
        snapshot=row["snapshot_json"] or {},
        created_at=created_at,
    )


async def _record_history(
    conn: asyncpg.Connection,
    config_id: UUID,
    event_type: str,
    snapshot: Dict[str, Any],
) -> None:
    payload = json.dumps(snapshot)
    await conn.execute(
        """
        INSERT INTO query_target_config_history (config_id, event_type, snapshot_json)
        VALUES ($1, $2, $3::jsonb)
        """,
        config_id,
        event_type,
        payload,
    )


def _row_to_snapshot(row: asyncpg.Record) -> Dict[str, Any]:
    def _safe_get(field: str):
        try:
            return row[field]
        except Exception:
            return None

    def _format_ts(value: Optional[datetime]) -> Optional[str]:
        return value.isoformat() if value else None

    return {
        "id": str(_safe_get("id")),
        "provider": _safe_get("provider"),
        "metadata": _safe_get("metadata") or {},
        "auth": _safe_get("auth") or {},
        "guardrails": _safe_get("guardrails") or {},
        "status": _safe_get("status"),
        "last_tested_at": _format_ts(_safe_get("last_tested_at")),
        "last_test_status": _safe_get("last_test_status"),
        "last_error_code": _safe_get("last_error_code"),
        "last_error_message": _safe_get("last_error_message"),
        "created_at": _format_ts(_safe_get("created_at")),
        "updated_at": _format_ts(_safe_get("updated_at")),
        "activated_at": _format_ts(_safe_get("activated_at")),
        "deactivated_at": _format_ts(_safe_get("deactivated_at")),
    }

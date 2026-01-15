import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from mcp_server.config.database import Database

from common.interfaces.pattern_run_store import PatternRunStore


class PostgresPatternRunStore(PatternRunStore):
    """Postgres implementation of PatternRunStore."""

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

    async def create_run(
        self,
        status: str,
        target_table: Optional[str] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> UUID:
        """Create a new run record."""
        config_json = json.dumps(config_snapshot) if config_snapshot else "{}"

        async with self._get_connection() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO nlp_pattern_runs (status, target_table, config_snapshot, started_at)
                VALUES ($1, $2, $3::jsonb, CURRENT_TIMESTAMP)
                RETURNING id
                """,
                status,
                target_table,
                config_json,
            )
            return row["id"]

    async def update_run(
        self,
        run_id: UUID,
        status: str,
        completed_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update an existing run record."""
        metrics_json = json.dumps(metrics) if metrics else None

        async with self._get_connection() as conn:
            # We construct the update logic to only update provided fields if possible,
            # but for this specific interface, we usually provide these at the end of a run.

            # Note: If metrics is None, we don't clobber it
            # (using COALESCE logic in SQL if strictly updating only what changed,
            # but here we might want to set it).
            # The interface suggests we are updating the run status.

            query = """
                UPDATE nlp_pattern_runs
                SET status = $2,
                    completed_at = COALESCE($3, completed_at),
                    error_message = COALESCE($4, error_message),
                    metrics = CASE WHEN $5::text IS NULL THEN metrics ELSE $5::jsonb END
                WHERE id = $1
            """
            await conn.execute(
                query,
                run_id,
                status,
                completed_at,
                error_message,
                metrics_json,
            )

    async def add_run_items(self, run_id: UUID, items: List[Dict[str, Any]]) -> None:
        """Add associated pattern items to a run."""
        if not items:
            return

        # Prepare data for executemany
        # items has keys: pattern_id, label, pattern, action
        data = [
            (
                run_id,
                str(item["pattern_id"]),
                item["label"],
                item["pattern"],
                item["action"],
            )
            for item in items
        ]

        async with self._get_connection() as conn:
            await conn.executemany(
                """
                INSERT INTO nlp_pattern_run_items (
                    run_id, pattern_id, pattern_label, pattern_text, action
                )
                VALUES ($1, $2, $3, $4, $5)
                """,
                data,
            )

    async def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List recent runs."""
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, started_at, completed_at, status, target_table, metrics
                FROM nlp_pattern_runs
                ORDER BY started_at DESC
                LIMIT $1
                """,
                limit,
            )
            return [dict(r) for r in rows]

    async def get_run(self, run_id: UUID) -> Optional[Dict[str, Any]]:
        """Get run details."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM nlp_pattern_runs WHERE id = $1
                """,
                run_id,
            )
            return dict(row) if row else None

    async def get_run_items(self, run_id: UUID) -> List[Dict[str, Any]]:
        """Get items associated with a run."""
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM nlp_pattern_run_items WHERE run_id = $1
                """,
                run_id,
            )
            return [dict(r) for r in rows]

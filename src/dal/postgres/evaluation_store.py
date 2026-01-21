"""Postgres implementation of EvaluationStore."""

import json
from contextlib import asynccontextmanager
from typing import List, Optional

from common.interfaces.evaluation_store import EvaluationStore
from dal.database import Database
from schema.evaluation.models import EvaluationCaseResultCreate, EvaluationRun, EvaluationRunCreate


class PostgresEvaluationStore(EvaluationStore):
    """Postgres implementation of EvaluationStore."""

    def __init__(self, db_client=None):
        """Initialize the store with an optional DB client."""
        self.db = db_client

    @asynccontextmanager
    async def _get_connection(self):
        if self.db:
            yield self.db
        else:
            async with Database.get_connection() as conn:
                yield conn

    async def create_run(self, run: EvaluationRunCreate) -> EvaluationRun:
        """Create a new evaluation run record."""
        config_json = json.dumps(run.config_snapshot)

        async with self._get_connection() as conn:
            # We use text ID if provided, or generate UUID if we were to support it.
            # Ideally the caller provides an ID or we generate one.
            # For this implementation, let's assume the caller generates the ID or we return one.
            # But the interface returns EvaluationRun which has an ID.
            # Let's generate a UUID in SQL if not provided?
            # Actually, let's generate a run_id in code if needed, but for now
            # let's assume we rely on the DB default or a separate ID generator.
            # Wait, EvaluationRunCreate doesn't have ID. So we must generate it.
            # We'll rely on DB gen_random_uuid() cast to text, or just generate here.
            # Let's use the DB default for consistent ID generation if we change DBs.

            row = await conn.fetchrow(
                """
                INSERT INTO evaluation_runs (
                    dataset_mode, dataset_version, git_sha, tenant_id,
                    config_snapshot, status, started_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, 'RUNNING', CURRENT_TIMESTAMP)
                RETURNING id::text, started_at, status
                """,
                run.dataset_mode,
                run.dataset_version,
                run.git_sha,
                run.tenant_id,
                config_json,
            )

            return EvaluationRun(
                id=row["id"],
                dataset_mode=run.dataset_mode,
                dataset_version=run.dataset_version,
                git_sha=run.git_sha,
                tenant_id=run.tenant_id,
                config_snapshot=run.config_snapshot,
                started_at=row["started_at"],
                status=row["status"],
            )

    async def update_run(self, run: EvaluationRun) -> None:
        """Update an existing evaluation run."""
        metrics_json = json.dumps(run.metrics_summary) if run.metrics_summary else None

        async with self._get_connection() as conn:
            await conn.execute(
                """
                UPDATE evaluation_runs
                SET status = $2,
                    completed_at = $3,
                    metrics_summary = $4::jsonb,
                    error_message = $5
                WHERE id = $1
                """,
                run.id,
                run.status,
                run.completed_at,
                metrics_json,
                run.error_message,
            )

    async def get_run(self, run_id: str) -> Optional[EvaluationRun]:
        """Get a run by ID."""
        async with self._get_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT id::text, dataset_mode, dataset_version, git_sha, tenant_id,
                       config_snapshot, status, started_at, completed_at,
                       metrics_summary, error_message
                FROM evaluation_runs
                WHERE id = $1
                """,
                run_id,
            )
            if not row:
                return None

            return EvaluationRun(
                id=row["id"],
                dataset_mode=row["dataset_mode"],
                dataset_version=row["dataset_version"],
                git_sha=row["git_sha"],
                tenant_id=row["tenant_id"],
                config_snapshot=json.loads(row["config_snapshot"] or "{}"),
                status=row["status"],
                started_at=row["started_at"],
                completed_at=row["completed_at"],
                metrics_summary=(
                    json.loads(row["metrics_summary"]) if row["metrics_summary"] else None
                ),
                error_message=row["error_message"],
            )

    async def save_case_results(self, results: List[EvaluationCaseResultCreate]) -> None:
        """Save a batch of evaluation case results."""
        if not results:
            return

        data = [
            (
                r.run_id,
                r.test_id,
                r.question,
                r.generated_sql,
                r.is_correct,
                r.structural_score,
                r.error_message,
                r.execution_time_ms,
                json.dumps(r.raw_response),
                r.trace_id,
            )
            for r in results
        ]

        async with self._get_connection() as conn:
            await conn.executemany(
                """
                INSERT INTO evaluation_results (
                    run_id, test_id, question, generated_sql, is_correct,
                    structural_score, error_message, execution_time_ms, raw_response, trace_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
                """,
                data,
            )

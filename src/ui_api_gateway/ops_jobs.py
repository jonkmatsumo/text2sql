"""Ops Jobs client for the UI API Gateway.

This module provides an isolated interface for ops_jobs database operations,
decoupling the gateway from the shared DAL package. The gateway only needs
access to the ops_jobs table in the control-plane database.

Future improvement: This could be replaced with an internal API call to the
MCP server or a dedicated ops service, removing direct database access entirely.
"""

import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional
from uuid import UUID

import asyncpg

from common.config.env import get_env_bool, get_env_int, get_env_str

logger = logging.getLogger(__name__)


class OpsJobsClient:
    """Client for managing ops_jobs in the control-plane database.

    This provides a clean interface for ops job operations without depending
    on the shared DAL package. The gateway should use this instead of
    ControlPlaneDatabase directly.
    """

    _pool: Optional[asyncpg.Pool] = None
    _initialized: bool = False

    @classmethod
    async def init(cls) -> bool:
        """Initialize connection pool for ops_jobs operations.

        Returns:
            True if initialization succeeded, False otherwise.
        """
        if cls._pool is not None:
            return True

        db_host = get_env_str("CONTROL_DB_HOST")
        if not db_host:
            logger.warning("CONTROL_DB_HOST not set, ops_jobs functionality disabled")
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
                server_settings={"application_name": "ui_api_gateway_ops"},
            )
            cls._initialized = True
            logger.info("OpsJobsClient connected to control-plane DB")
            return True
        except Exception as e:
            logger.error("Failed to connect to control-plane DB: %s", e)
            return False

    @classmethod
    async def close(cls) -> None:
        """Close the connection pool."""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            cls._initialized = False
            logger.info("OpsJobsClient connection pool closed")

    @classmethod
    def is_available(cls) -> bool:
        """Check if ops_jobs functionality is available."""
        return cls._pool is not None

    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Get a connection from the pool.

        Yields:
            asyncpg Connection in a transaction context.

        Raises:
            RuntimeError: If the pool is not initialized.
        """
        if cls._pool is None:
            raise RuntimeError("OpsJobsClient not initialized. Call OpsJobsClient.init() first.")

        async with cls._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    @classmethod
    async def create_job(cls, job_id: UUID, job_type: str) -> None:
        """Create a new ops job record.

        Args:
            job_id: Unique identifier for the job.
            job_type: Type of operation (e.g., SCHEMA_HYDRATION, CACHE_REINDEX).
        """
        async with cls.get_connection() as conn:
            await conn.execute(
                "INSERT INTO ops_jobs (id, job_type, status) VALUES ($1, $2, 'PENDING')",
                job_id,
                job_type,
            )

    @classmethod
    async def update_status(
        cls,
        job_id: UUID,
        status: str,
        error_message: Optional[str] = None,
        result: Optional[Any] = None,
    ) -> None:
        """Update job status.

        Args:
            job_id: Job identifier.
            status: New status (RUNNING, COMPLETED, FAILED).
            error_message: Error message if status is FAILED.
            result: Result data to store.
        """
        async with cls.get_connection() as conn:
            if status == "RUNNING":
                await conn.execute(
                    "UPDATE ops_jobs SET status = 'RUNNING' WHERE id = $1",
                    job_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE ops_jobs
                    SET status = $2, finished_at = NOW(), error_message = $3, result = $4
                    WHERE id = $1
                    """,
                    job_id,
                    status,
                    error_message,
                    json.dumps(result) if result and not isinstance(result, str) else result,
                )

    @classmethod
    async def update_progress(
        cls,
        job_id: UUID,
        progress: dict,
    ) -> None:
        """Update job progress metadata.

        Args:
            job_id: Job identifier.
            progress: Progress metadata to merge into result.
        """
        async with cls.get_connection() as conn:
            await conn.execute(
                """
                UPDATE ops_jobs
                SET result = COALESCE(result, '{}'::jsonb) || $2::jsonb
                WHERE id = $1
                """,
                job_id,
                json.dumps(progress),
            )

    @classmethod
    async def get_job(cls, job_id: UUID) -> Optional[dict]:
        """Fetch job status by ID.

        Args:
            job_id: Job identifier.

        Returns:
            Job record as dict, or None if not found.
        """
        async with cls.get_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM ops_jobs WHERE id = $1", job_id)
            if not row:
                return None

            result = row["result"]
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except Exception:
                    result = {"raw": result}

            return {
                "id": row["id"],
                "job_type": row["job_type"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row.get("finished_at"),
                "error_message": row.get("error_message"),
                "result": result or {},
            }

    @classmethod
    async def list_jobs(
        cls, limit: int = 50, job_type: Optional[str] = None, status: Optional[str] = None
    ) -> list[dict]:
        """List jobs with optional filtering.

        Args:
            limit: Maximum number of jobs to return.
            job_type: Filter by job type.
            status: Filter by job status.

        Returns:
            List of job records.
        """
        query = "SELECT * FROM ops_jobs"
        conditions = []
        params = []

        if job_type:
            params.append(job_type)
            conditions.append(f"job_type = ${len(params)}")

        if status:
            params.append(status)
            conditions.append(f"status = ${len(params)}")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY started_at DESC"
        params.append(limit)
        query += f" LIMIT ${len(params)}"

        async with cls.get_connection() as conn:
            rows = await conn.fetch(query, *params)
            jobs = []
            for row in rows:
                result = row["result"]
                if isinstance(result, str):
                    try:
                        result = json.loads(result)
                    except Exception:
                        result = {"raw": result}

                jobs.append(
                    {
                        "id": row["id"],
                        "job_type": row["job_type"],
                        "status": row["status"],
                        "started_at": row["started_at"],
                        "finished_at": row.get("finished_at"),
                        "error_message": row.get("error_message"),
                        "result": result or {},
                    }
                )
            return jobs


# Feature flag for using legacy DAL path vs new isolated client
USE_LEGACY_DAL = get_env_bool("GATEWAY_USE_LEGACY_DAL", True)


def use_legacy_dal() -> bool:
    """Check if gateway should use legacy DAL path.

    Returns:
        True to use ControlPlaneDatabase, False to use OpsJobsClient.
    """
    return USE_LEGACY_DAL

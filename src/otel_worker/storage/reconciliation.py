"""Reconciliation logic for identifying and cleaning up orphan telemetry artifacts."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import text

from otel_worker.config import settings
from otel_worker.storage.minio import client as minio_client

logger = logging.getLogger(__name__)


def find_orphan_minio_objects(postgres_engine, age_minutes: int = 60) -> List[str]:
    """Find objects in MinIO that have no matching record in Postgres.

    Orphans are objects older than age_minutes.
    """
    orphans = []
    threshold = datetime.now(timezone.utc) - timedelta(minutes=age_minutes)

    # List objects in the OTEL bucket
    # Note: This is an expensive operation if many objects exist.
    # In a real system, we'd use MinIO notifications or a more specialized index.
    objects = minio_client.list_objects(
        settings.MINIO_BUCKET, prefix=f"{settings.OTEL_ENVIRONMENT}/", recursive=True
    )

    # We only care about trace blobs for this check
    # Format: {env}/{service}/{date}/{trace_id}.json.gz
    for obj in objects:
        if not obj.object_name.endswith(".json.gz"):
            continue

        if obj.last_modified > threshold:
            # Too new, might still be in processing
            continue

        # Extract trace_id from filename
        filename = obj.object_name.split("/")[-1]
        trace_id = filename.split(".")[0]

        # Check if trace exists in PG
        if not _trace_exists_in_pg(postgres_engine, trace_id):
            orphans.append(obj.object_name)

    return orphans


def _trace_exists_in_pg(engine, trace_id: str) -> bool:
    """Check if a trace_id exists in the traces table."""
    from otel_worker.storage.postgres import get_table_name

    traces_table = get_table_name("traces")

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT 1 FROM {traces_table} WHERE trace_id = :tid"), {"tid": trace_id}
        )
        return result.fetchone() is not None


def cleanup_orphans(orphans: List[str]):
    """Delete identified orphan objects from MinIO."""
    for obj_name in orphans:
        try:
            minio_client.remove_object(settings.MINIO_BUCKET, obj_name)
            logger.info(f"Cleaned up orphan MinIO object: {obj_name}")
        except Exception as e:
            logger.error(f"Failed to delete orphan {obj_name}: {e}")


def run_reconciliation(engine, age_minutes: int = 60):
    """Orchestrate the reconciliation and cleanup process."""
    logger.info(f"Starting orphan reconciliation (Age > {age_minutes}m)")
    try:
        orphans = find_orphan_minio_objects(engine, age_minutes=age_minutes)
        if orphans:
            logger.info(f"Found {len(orphans)} orphans. Starting cleanup.")
            cleanup_orphans(orphans)
        else:
            logger.info("No orphans found.")
        return {"orphans_found": len(orphans)}
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        raise


class ReconciliationCoordinator:
    """Background task for periodic orphan cleanup."""

    def __init__(self, postgres_engine, interval_seconds: int = 3600):
        """Initialize with engine and interval."""
        self.engine = postgres_engine
        self.interval_seconds = interval_seconds
        self._task = None
        self._stopping = False

    async def start(self):
        """Start the background task."""
        if self._task and not self._task.done():
            logger.debug("Reconciliation coordinator already running")
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run())
        logger.info(f"Reconciliation coordinator started (Interval: {self.interval_seconds}s)")

    async def stop(self):
        """Stop the background task."""
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
        logger.info("Reconciliation coordinator stopped")

    async def _run(self):
        """Loop that runs reconciliation periodically."""
        while not self._stopping:
            try:
                await asyncio.to_thread(run_reconciliation, self.engine)
            except Exception as e:
                logger.error(f"Error in background reconciliation: {e}")

            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break


from otel_worker.storage.postgres import engine as pg_engine  # noqa: E402

reconciliation_coordinator = ReconciliationCoordinator(pg_engine)

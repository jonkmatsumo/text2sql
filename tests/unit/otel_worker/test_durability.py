"""Tests for ingestion durability and orphan reconciliation."""

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

# Set dummy URL before importing modules that initialize engines
os.environ["POSTGRES_URL"] = "postgresql://user:pass@localhost/dbname"

from otel_worker.storage.postgres import SafeIngestQueue  # noqa: E402
from otel_worker.storage.reconciliation import find_orphan_minio_objects  # noqa: E402


class TestDurability(unittest.TestCase):
    """Tests for durable ingestion and orphan cleanup."""

    def test_safe_ingest_queue_fallback(self):
        """Test that SafeIngestQueue buffers in memory if Postgres fails."""
        queue = SafeIngestQueue(max_buffer_size=10)
        payload = {"test": "data"}

        # Mock enqueue_ingestion_direct to fail
        with patch(
            "otel_worker.storage.postgres.enqueue_ingestion_direct",
            side_effect=Exception("DB Down"),
        ):
            result = queue.enqueue(payload, trace_id="t1")

            self.assertEqual(result, -1)
            self.assertEqual(len(queue.buffer), 1)
            self.assertEqual(queue.buffer[0]["trace_id"], "t1")

    def test_safe_ingest_queue_flushes_buffer_on_stop(self):
        """Stopping the queue should synchronously flush recoverable buffered items."""
        queue = SafeIngestQueue(max_buffer_size=10)
        payload = {"test": "data"}

        with patch(
            "otel_worker.storage.postgres.enqueue_ingestion_direct",
            side_effect=[Exception("DB Down"), 1],
        ):
            queue.enqueue(payload, trace_id="t1")
            self.assertEqual(len(queue.buffer), 1)
            queue.stop()

        self.assertEqual(len(queue.buffer), 0)

    def test_find_orphan_minio_objects(self):
        """Test orphan identification logic."""
        mock_engine = MagicMock()
        mock_minio = MagicMock()

        # Mock objects in MinIO
        # 1. New object (not orphan)
        # 2. Old object with PG record (not orphan)
        # 3. Old object without PG record (ORPHAN)

        now = datetime.now(timezone.utc)

        obj1 = MagicMock()
        obj1.object_name = "local/svc/2026-01-28/new.json.gz"
        obj1.last_modified = now - timedelta(minutes=5)

        obj2 = MagicMock()
        obj2.object_name = "local/svc/2026-01-28/exists.json.gz"
        obj2.last_modified = now - timedelta(minutes=70)

        obj3 = MagicMock()
        obj3.object_name = "local/svc/2026-01-28/orphan.json.gz"
        obj3.last_modified = now - timedelta(minutes=70)

        mock_minio.list_objects.return_value = [obj1, obj2, obj3]

        # Mock PG check
        def mock_exists(engine, trace_id):
            return trace_id == "exists"

        with patch("otel_worker.storage.reconciliation.minio_client", mock_minio):
            with patch(
                "otel_worker.storage.reconciliation._trace_exists_in_pg", side_effect=mock_exists
            ):
                orphans = find_orphan_minio_objects(mock_engine, age_minutes=60)

                self.assertEqual(len(orphans), 1)
                self.assertEqual(orphans[0], obj3.object_name)

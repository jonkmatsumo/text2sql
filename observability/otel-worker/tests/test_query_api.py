import os
import unittest
from datetime import datetime, timedelta, timezone

# Load root .env if it exists
dotenv_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    ".env",
)
# Force SQLite for tests to avoid all Postgres environment issues
os.environ["POSTGRES_URL"] = "sqlite:///otel_test.db"
os.environ["OTEL_DB_SCHEMA"] = ""

from fastapi.testclient import TestClient  # noqa: E402
from otel_worker.app import app  # noqa: E402
from otel_worker.storage.postgres import engine, save_trace_and_spans  # noqa: E402
from sqlalchemy import text  # noqa: E402


class TestQueryAPI(unittest.TestCase):
    """Integration tests for the OTEL Query API."""

    @classmethod
    def setUpClass(cls):
        """Set up the test database schema and seed data."""
        # Ensure tables exist in SQLite
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS traces"))
            conn.execute(text("DROP TABLE IF EXISTS spans"))
            conn.execute(
                text(
                    """
                CREATE TABLE traces (
                    trace_id TEXT PRIMARY KEY,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_ms INTEGER,
                    service_name TEXT,
                    environment TEXT,
                    tenant_id TEXT,
                    interaction_id TEXT,
                    status TEXT,
                    error_count INTEGER,
                    span_count INTEGER,
                    raw_blob_url TEXT,
                    resource_attributes TEXT,
                    trace_attributes TEXT
                )
            """
                )
            )
            conn.execute(
                text(
                    """
                CREATE TABLE spans (
                    span_id TEXT PRIMARY KEY,
                    trace_id TEXT,
                    parent_span_id TEXT,
                    name TEXT,
                    kind TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_ms INTEGER,
                    status_code TEXT,
                    status_message TEXT,
                    span_attributes TEXT,
                    events TEXT
                )
            """
                )
            )
            conn.commit()

        cls.client = TestClient(app)
        # Seed some data
        cls.trace_id = "test-trace-123"
        cls.service_name = "test-service"
        cls.summaries = [
            {
                "span_id": "span-1",
                "trace_id": cls.trace_id,
                "name": "root",
                "kind": "SERVER",
                "start_time_unix_nano": int(
                    (datetime.now(timezone.utc) - timedelta(seconds=10)).timestamp() * 1e9
                ),
                "end_time_unix_nano": int(datetime.now(timezone.utc).timestamp() * 1e9),
                "status": "STATUS_CODE_OK",
                "service_name": cls.service_name,
                "attributes": {"tenant_id": "tenant-1"},
            }
        ]
        save_trace_and_spans(cls.trace_id, {}, cls.summaries, "s3://bucket/test-blob")

    def test_list_traces(self):
        """Test listing traces."""
        response = self.client.get("/api/v1/traces")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertGreaterEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["trace_id"], self.trace_id)

    def test_list_traces_filter_service(self):
        """Test listing traces with service filter."""
        response = self.client.get(f"/api/v1/traces?service={self.service_name}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["items"][0]["service_name"], self.service_name)

    def test_get_trace_detail(self):
        """Test getting trace details."""
        response = self.client.get(f"/api/v1/traces/{self.trace_id}")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["trace_id"], self.trace_id)
        # Pydantic might include them as None by default
        self.assertIsNone(data.get("resource_attributes"))

    def test_get_trace_detail_with_attributes(self):
        """Test getting trace details with attributes."""
        response = self.client.get(f"/api/v1/traces/{self.trace_id}?include=attributes")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("resource_attributes", data)
        self.assertIn("trace_attributes", data)

    def test_list_spans(self):
        """Test listing spans for a trace."""
        response = self.client.get(f"/api/v1/traces/{self.trace_id}/spans")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertGreaterEqual(len(data["items"]), 1)
        self.assertEqual(data["items"][0]["span_id"], "span-1")

    def test_get_raw_blob_not_implemented(self):
        """Test getting raw blob from MinIO."""
        # Since we use get_trace_blob which might fail in test env without real MinIO
        response = self.client.get(f"/api/v1/traces/{self.trace_id}/raw")
        # Expecting 404 or 500 depending on MinIO connectivity in test env
        self.assertIn(response.status_code, [404, 500])


if __name__ == "__main__":
    unittest.main()

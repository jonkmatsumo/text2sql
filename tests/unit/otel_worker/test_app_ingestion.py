import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from otel_worker.app import app


class TestOTELWorkerIngestion(unittest.TestCase):
    """Integration tests for the OTEL worker trace ingestion endpoint."""

    def setUp(self):
        """Set up the test client and dummy environment."""
        self.client = TestClient(app)

    @patch("otel_worker.app.enqueue_ingestion")
    def test_receive_json_traces(self, mock_enqueue):
        """Verify that JSON traces are correctly enqueued."""
        response = self.client.post(
            "/v1/traces", content='{"test": 1}', headers={"Content-Type": "application/json"}
        )

        self.assertEqual(response.status_code, 202)
        mock_enqueue.assert_called_once()

    @patch("otel_worker.app.enqueue_ingestion")
    def test_receive_proto_traces(self, mock_enqueue):
        """Verify that Protobuf traces are correctly enqueued."""
        response = self.client.post(
            "/v1/traces", content=b"\x00", headers={"Content-Type": "application/x-protobuf"}
        )

        self.assertEqual(response.status_code, 202)
        mock_enqueue.assert_called_once()

    def test_receive_unsupported_type(self):
        """Verify that unsupported content-types result in a 415 error."""
        response = self.client.post(
            "/v1/traces", content="text", headers={"Content-Type": "text/plain"}
        )

        self.assertEqual(response.status_code, 415)
        self.assertIn("Unsupported content-type", response.text)

    @patch("otel_worker.app.enqueue_ingestion")
    def test_receive_malformed_json(self, mock_enqueue):
        """Verify that malformed JSON is still enqueued (best-effort)."""
        response = self.client.post(
            "/v1/traces", content="{bad json", headers={"Content-Type": "application/json"}
        )

        self.assertEqual(response.status_code, 202)
        mock_enqueue.assert_called_once()


if __name__ == "__main__":
    unittest.main()

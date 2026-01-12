import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from otel_worker.app import app


class TestOTELWorkerIngestion(unittest.TestCase):
    """Integration tests for the OTEL worker trace ingestion endpoint."""

    def setUp(self):
        """Set up the test client and dummy environment."""
        self.client = TestClient(app)

    @patch("otel_worker.app.parse_otlp_json_traces")
    @patch("otel_worker.app.extract_trace_summaries")
    def test_receive_json_traces(self, mock_extract, mock_parse):
        """Verify that JSON traces are correctly routed and parsed."""
        mock_parse.return_value = {"resourceSpans": []}
        mock_extract.return_value = []

        response = self.client.post(
            "/v1/traces", content="{}", headers={"Content-Type": "application/json"}
        )

        # Empty summaries return 200 OK
        self.assertEqual(response.status_code, 200)
        mock_parse.assert_called_once()

    @patch("otel_worker.app.parse_otlp_json_traces")
    @patch("otel_worker.app.extract_trace_summaries")
    @patch("otel_worker.app.coordinator.enqueue")
    def test_receive_json_traces_enqueued(self, mock_enqueue, mock_extract, mock_parse):
        """Verify that non-empty JSON traces return 202 Accepted."""
        mock_parse.return_value = {"resourceSpans": []}
        mock_extract.return_value = [{"trace_id": "123", "service_name": "test"}]

        response = self.client.post(
            "/v1/traces", content="{}", headers={"Content-Type": "application/json"}
        )

        self.assertEqual(response.status_code, 202)
        mock_enqueue.assert_called_once()

    @patch("otel_worker.app.parse_otlp_traces")
    @patch("otel_worker.app.extract_trace_summaries")
    def test_receive_proto_traces(self, mock_extract, mock_parse):
        """Verify that Protobuf traces are correctly routed and parsed."""
        mock_parse.return_value = {"resourceSpans": []}
        mock_extract.return_value = []

        response = self.client.post(
            "/v1/traces", content=b"\x00", headers={"Content-Type": "application/x-protobuf"}
        )

        # Empty summaries return 200 OK
        self.assertEqual(response.status_code, 200)
        mock_parse.assert_called_once()

    def test_receive_unsupported_type(self):
        """Verify that unsupported content-types result in a 415 error."""
        response = self.client.post(
            "/v1/traces", content="text", headers={"Content-Type": "text/plain"}
        )

        self.assertEqual(response.status_code, 415)
        self.assertIn("Unsupported content-type", response.text)

    def test_receive_malformed_json(self):
        """Verify that malformed JSON results in a 400 error."""
        response = self.client.post(
            "/v1/traces", content="{bad json", headers={"Content-Type": "application/json"}
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid OTLP JSON payload", response.text)


if __name__ == "__main__":
    unittest.main()

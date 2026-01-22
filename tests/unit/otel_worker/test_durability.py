"""Unit tests for the persistence coordinator."""

import unittest
from unittest.mock import AsyncMock, patch

from otel_worker.ingestion.processor import PersistenceCoordinator


class TestPersistenceDurability(unittest.IsolatedAsyncioTestCase):
    """Integration tests for the persistence durability logic."""

    async def asyncSetUp(self):
        """Set up a fresh coordinator for each test."""
        # Initialize coordinator with very short delay for testing
        self.coordinator = PersistenceCoordinator(max_attempts=3, initial_delay=0.01)

    async def test_retry_on_transient_failure(self):
        """Verify that a sink is retried on failure."""
        mock_sink = AsyncMock()
        # Fail twice, succeed on third attempt
        mock_sink.side_effect = [
            Exception("Transient error"),
            Exception("Transient error"),
            "Success",
        ]

        await self.coordinator._run_with_retry(mock_sink, "TestSink")

        self.assertEqual(mock_sink.call_count, 3)

    async def test_stop_after_max_attempts(self):
        """Verify that retries stop after max_attempts."""
        mock_sink = AsyncMock()
        mock_sink.side_effect = Exception("Permanent error")

        with self.assertRaises(Exception):
            await self.coordinator._run_with_retry(mock_sink, "TestSink")

        self.assertEqual(mock_sink.call_count, 3)

    @patch("otel_worker.ingestion.processor.asyncio.to_thread")
    async def test_sink_isolation(self, mock_to_thread):
        """Verify that one sink failure does not block another."""

        def side_effect(func, *args, **kwargs):
            if func.__name__ == "export_to_mlflow":
                raise Exception("MLflow is down")
            return "ok"

        mock_to_thread.side_effect = side_effect

        parsed_data = {"resourceSpans": []}
        summaries = [{"trace_id": "abc", "service_name": "test"}]

        # This should complete despite the MLflow error
        await self.coordinator._process_trace("trace-1", "test", parsed_data, summaries)

        self.assertGreaterEqual(mock_to_thread.call_count, 3)

    async def test_queue_backpressure(self):
        """Verify that enqueueing works as expected."""
        await self.coordinator.enqueue({}, [{"trace_id": "1", "service_name": "s"}])
        self.assertEqual(self.coordinator.queue.qsize(), 1)


if __name__ == "__main__":
    unittest.main()

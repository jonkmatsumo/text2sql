"""Unit tests for the persistence coordinator."""

import unittest
from unittest.mock import AsyncMock

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


if __name__ == "__main__":
    unittest.main()

"""Background coordinator for metrics aggregation."""

import asyncio
import logging

from otel_worker.metrics.aggregate import run_aggregation
from otel_worker.storage.postgres import engine

logger = logging.getLogger(__name__)


class AggregationCoordinator:
    """Manages background aggregation of trace metrics."""

    def __init__(self, interval_seconds: int = 60, lookback_minutes: int = 60):
        """Initialize the coordinator."""
        self.interval_seconds = interval_seconds
        self.lookback_minutes = lookback_minutes
        self._task: asyncio.Task = None
        self._stopping = False

    async def start(self):
        """Start the background aggregation loop."""
        if self._task:
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run_loop())
        logger.info(f"Started AggregationCoordinator (Interval: {self.interval_seconds}s)")

    async def stop(self):
        """Stop the background loop."""
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Stopped AggregationCoordinator")

    async def _run_loop(self):
        """Run the aggregation job periodically."""
        while not self._stopping:
            try:
                # Run synchronous aggregation in a thread to verify it doesn't block the event loop
                await asyncio.to_thread(
                    run_aggregation,
                    engine,
                    lookback_minutes=self.lookback_minutes,
                    batch_size=100,  # Batch size can be tuned if needed
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in aggregation loop: {e}")

            # Sleep for the interval
            try:
                await asyncio.sleep(self.interval_seconds)
            except asyncio.CancelledError:
                break


# Global coordinator instance
aggregation_coordinator = AggregationCoordinator()

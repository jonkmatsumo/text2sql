import asyncio
import logging
import random
from enum import Enum
from typing import Optional

from otel_worker.config import settings
from otel_worker.storage.postgres import get_queue_depth

logger = logging.getLogger(__name__)


class OverflowAction(Enum):
    """Actions to take when queue is overflowing."""

    ACCEPT = "accept"
    DROP = "drop"
    REJECT = "reject"


class QueueMonitor:
    """Monitors ingestion queue depth to enforce overflow policies."""

    def __init__(self, poll_interval: float = 1.0):
        """Initialize queue monitor."""
        self.poll_interval = poll_interval
        self._current_depth = 0
        self._monitoring_task: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self):
        """Start background monitoring."""
        self._stopping = False
        self._monitoring_task = asyncio.create_task(self._monitor_loop())
        logger.info(
            f"Queue monitor started (Max Backlog: {settings.STAGING_MAX_BACKLOG}, "
            f"Policy: {settings.OVERFLOW_POLICY})"
        )

    async def stop(self):
        """Stop background monitoring."""
        self._stopping = True
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Queue monitor stopped")

    def check_admissibility(self) -> OverflowAction:
        """Determine if a request should be accepted based on current depth and policy."""
        # Fast path: under limit
        if self._current_depth < settings.STAGING_MAX_BACKLOG:
            return OverflowAction.ACCEPT

        # Slow path: apply policy
        policy = settings.OVERFLOW_POLICY.lower()

        if policy == "reject":
            return OverflowAction.REJECT

        if policy == "sample":
            if random.random() < settings.OVERFLOW_SAMPLE_RATE:
                return OverflowAction.ACCEPT
            else:
                return OverflowAction.DROP  # Drop silently (202 Accepted but not enqueued)

        # Default to drop (or explicit 'drop' policy)
        return OverflowAction.DROP

    async def _monitor_loop(self):
        """Periodically polls queue depth."""
        while not self._stopping:
            try:
                # Run sync DB call in thread
                depth = await asyncio.to_thread(get_queue_depth)
                self._current_depth = depth
            except Exception as e:
                logger.error(f"Failed to poll queue depth: {e}")

            await asyncio.sleep(self.poll_interval)


# Global monitor instance
monitor = QueueMonitor()

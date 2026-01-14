from unittest.mock import patch

import pytest
from otel_worker.config import settings
from otel_worker.ingestion.monitor import OverflowAction, QueueMonitor


@pytest.fixture
def monitor():
    """Create a QueueMonitor fixture."""
    return QueueMonitor()


def test_under_limit(monitor):
    """Test that requests are accepted when under the limit."""
    settings.STAGING_MAX_BACKLOG = 100
    monitor._current_depth = 50
    assert monitor.check_admissibility() == OverflowAction.ACCEPT


def test_policy_reject(monitor):
    """Test 'reject' policy behavior."""
    settings.STAGING_MAX_BACKLOG = 100
    settings.OVERFLOW_POLICY = "reject"
    monitor._current_depth = 150
    assert monitor.check_admissibility() == OverflowAction.REJECT


def test_policy_drop(monitor):
    """Test 'drop' policy behavior."""
    settings.STAGING_MAX_BACKLOG = 100
    settings.OVERFLOW_POLICY = "drop"
    monitor._current_depth = 150
    assert monitor.check_admissibility() == OverflowAction.DROP


def test_policy_sample(monitor):
    """Test 'sample' policy behavior (probabilistic)."""
    settings.STAGING_MAX_BACKLOG = 100
    settings.OVERFLOW_POLICY = "sample"
    settings.OVERFLOW_SAMPLE_RATE = 0.5
    monitor._current_depth = 150

    # Mock random to force ACCEPT
    with patch("random.random", return_value=0.1):
        assert monitor.check_admissibility() == OverflowAction.ACCEPT

    # Mock random to force DROP
    with patch("random.random", return_value=0.9):
        assert monitor.check_admissibility() == OverflowAction.DROP


@pytest.mark.asyncio
async def test_monitor_loop():
    """Test that the monitor loop updates depth."""
    monitor = QueueMonitor(poll_interval=0.1)

    with patch("otel_worker.ingestion.monitor.get_queue_depth", return_value=999):
        await monitor.start()
        # Give it a moment to poll
        try:
            # Wait for a brief period to allow the task to run
            import asyncio

            await asyncio.sleep(0.2)

            assert monitor._current_depth == 999
        finally:
            await monitor.stop()

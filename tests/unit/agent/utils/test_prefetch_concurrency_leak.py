"""Tests for prefetch counter leaks under failure and cancellation."""

import asyncio

import pytest

from agent.utils.pagination_prefetch import (
    PrefetchManager,
    prefetch_diagnostics,
    reset_prefetch_state,
)


@pytest.fixture(autouse=True)
def _reset_prefetch_fixture():
    reset_prefetch_state()
    yield
    reset_prefetch_state()


@pytest.mark.asyncio
async def test_prefetch_counters_do_not_leak_on_exception():
    """Verify that active and waiting counters return to zero on task failure."""

    async def _fail_fetch():
        raise RuntimeError("Task failed")

    pm = PrefetchManager(max_concurrency=1)
    async with pm:
        pm.schedule("key-1", _fail_fetch)
        # Wait a bit for the task to run and fail
        await asyncio.sleep(0.05)

    diag = prefetch_diagnostics()
    assert diag["active_count"] == 0, "Active count leaked after failure"
    assert diag["waiting_count"] == 0, "Waiting count leaked after failure"


@pytest.mark.asyncio
async def test_prefetch_counters_do_not_leak_on_cancellation():
    """Verify that counters return to zero if the task is cancelled."""
    start_event = asyncio.Event()

    async def _cancelled_fetch():
        start_event.set()
        try:
            await asyncio.sleep(10)  # Wait to be cancelled
        except asyncio.CancelledError:
            raise

    pm = PrefetchManager(max_concurrency=1)

    # Run the manager in a background task so we can cancel it
    async def _run_manager():
        async with pm:
            pm.schedule("key-1", _cancelled_fetch)
            await start_event.wait()
            # Task is now "active"
            raise asyncio.CancelledError()  # Simulate manager cancellation

    task = asyncio.create_task(_run_manager())
    await asyncio.sleep(0.05)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    diag = prefetch_diagnostics()
    # Note: Because the manager's TaskGroup is cancelled, it cancels all children.
    # We want to ensure the counters are cleaned up.
    assert diag["active_count"] == 0, "Active count leaked after cancellation"
    assert diag["waiting_count"] == 0, "Waiting count leaked after cancellation"

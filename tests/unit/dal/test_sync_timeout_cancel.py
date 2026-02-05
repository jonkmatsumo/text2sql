"""Tests for sync timeout cancellation helper."""

import asyncio

import pytest

from dal.capabilities import capabilities_for_provider
from dal.util.timeouts import run_with_timeout


@pytest.mark.asyncio
async def test_run_with_timeout_invokes_cancel():
    """Timeouts should invoke cancellation hook."""
    cancel_called = False

    async def _operation():
        await asyncio.sleep(0.2)
        return "ok"

    async def _cancel():
        nonlocal cancel_called
        cancel_called = True

    with pytest.raises(asyncio.TimeoutError):
        await run_with_timeout(_operation, timeout_seconds=0.01, cancel=_cancel)

    assert cancel_called is True


def test_sync_provider_capabilities_cancel_support():
    """Sync providers should advertise cancel support when available."""
    assert capabilities_for_provider("sqlite").supports_cancel is True
    assert capabilities_for_provider("postgres").supports_cancel is True

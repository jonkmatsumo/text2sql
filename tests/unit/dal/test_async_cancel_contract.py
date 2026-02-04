"""Tests for async timeout cancellation behavior."""

import asyncio

import pytest

from dal.async_utils import with_timeout


@pytest.mark.asyncio
async def test_cancel_on_timeout_called_once():
    """Timeout triggers cancellation handler once."""
    calls = {"count": 0}

    async def _on_timeout():
        calls["count"] += 1

    with pytest.raises(asyncio.TimeoutError):
        await with_timeout(asyncio.sleep(0.01), timeout_seconds=0.001, on_timeout=_on_timeout)

    assert calls["count"] == 1


@pytest.mark.asyncio
async def test_cancel_exception_does_not_mask_timeout(caplog):
    """Cancellation errors are logged without masking timeout."""

    async def _on_timeout():
        raise RuntimeError("cancel failed")

    with pytest.raises(asyncio.TimeoutError):
        await with_timeout(asyncio.sleep(0.01), timeout_seconds=0.001, on_timeout=_on_timeout)

    assert any("Timeout cancellation failed" in r.message for r in caplog.records)

"""Unit tests for async timeout helper."""

import asyncio

import pytest

from dal.async_utils import with_timeout


@pytest.mark.asyncio
async def test_with_timeout_returns_result():
    """Verify with_timeout returns the awaitable result."""
    result = await with_timeout(asyncio.sleep(0, result="ok"), timeout_seconds=1)
    assert result == "ok"


@pytest.mark.asyncio
async def test_with_timeout_calls_handler_on_timeout():
    """Verify timeout handler is invoked on timeout."""
    called = False

    async def _handler():
        nonlocal called
        called = True

    with pytest.raises(asyncio.TimeoutError):
        await with_timeout(asyncio.sleep(0.05), timeout_seconds=0.001, on_timeout=_handler)

    assert called is True

import asyncio
import logging
from typing import Awaitable, Callable, Optional


async def with_timeout(
    awaitable: Awaitable,
    timeout_seconds: float,
    on_timeout: Optional[Callable[[], Awaitable[None]]] = None,
):
    """Run an awaitable with a timeout and optional timeout handler."""
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        if on_timeout is not None:
            try:
                await on_timeout()
            except Exception as exc:
                logger = logging.getLogger(__name__)
                logger.warning("Timeout cancellation failed: %s", exc)
        raise

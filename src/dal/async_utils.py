import asyncio
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
            await on_timeout()
        raise

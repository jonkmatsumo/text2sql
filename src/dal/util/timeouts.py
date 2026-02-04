import asyncio
import inspect
from typing import Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")


async def run_with_timeout(
    operation: Callable[[], Awaitable[T]],
    timeout_seconds: Optional[float],
    cancel: Optional[Callable[[], Awaitable[None]]] = None,
) -> T:
    """Run an awaitable operation with a timeout and optional cancellation."""
    if not timeout_seconds or timeout_seconds <= 0:
        return await operation()
    try:
        return await asyncio.wait_for(operation(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        if cancel:
            result = cancel()
            if inspect.isawaitable(result):
                await result
        raise

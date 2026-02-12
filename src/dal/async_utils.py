from typing import Awaitable, Callable, Optional

from dal.util.timeouts import run_with_timeout


async def with_timeout(
    awaitable: Awaitable,
    timeout_seconds: float,
    on_timeout: Optional[Callable[[], Awaitable[None]]] = None,
):
    """Run an awaitable with a timeout and optional timeout handler."""

    async def _operation():
        return await awaitable

    return await run_with_timeout(
        _operation,
        timeout_seconds=timeout_seconds,
        cancel=on_timeout,
        operation_name="operation",
    )

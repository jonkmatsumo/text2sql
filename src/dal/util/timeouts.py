import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")
logger = logging.getLogger(__name__)


class QueryTimeoutError(TimeoutError):
    """Canonical DAL timeout error with provider and operation context."""

    def __init__(
        self, provider: str, operation_name: str, timeout_seconds: Optional[float]
    ) -> None:
        """Initialize timeout details with provider/operation context."""
        self.provider = provider
        self.operation_name = operation_name
        self.timeout_seconds = timeout_seconds
        timeout_display = "unknown"
        if isinstance(timeout_seconds, (int, float)):
            timeout_display = f"{float(timeout_seconds):g}"
        super().__init__(f"{provider} {operation_name} timed out after {timeout_display}s.")


async def run_with_timeout(
    operation: Callable[[], Awaitable[T]],
    timeout_seconds: Optional[float],
    cancel: Optional[Callable[[], Awaitable[None]]] = None,
    *,
    provider: str = "unknown",
    operation_name: str = "operation",
) -> T:
    """Run an awaitable operation with a timeout and optional cancellation."""
    if not timeout_seconds or timeout_seconds <= 0:
        return await operation()
    try:
        return await asyncio.wait_for(operation(), timeout=timeout_seconds)
    except asyncio.TimeoutError as exc:
        if cancel:
            try:
                result = cancel()
                if inspect.isawaitable(result):
                    await result
            except Exception as cancel_exc:
                logger.warning("Timeout cancellation failed: %s", cancel_exc)
        raise QueryTimeoutError(
            provider=provider,
            operation_name=operation_name,
            timeout_seconds=timeout_seconds,
        ) from exc

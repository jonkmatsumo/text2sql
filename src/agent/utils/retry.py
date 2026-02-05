"""Retry utility for transient database errors.

Provides exponential backoff with jitter for retrying transient failures.
"""

import asyncio
import logging
import random
from typing import Callable, Optional, TypeVar

from agent.telemetry import telemetry
from common.config.env import get_env_bool, get_env_str
from dal.error_classification import ErrorClassification, classify_error_info

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Transient error types that are safe to retry
# These cover common asyncpg/postgres transient failures
TRANSIENT_ERROR_PATTERNS = {
    "connection",
    "timeout",
    "temporarily unavailable",
    "connection refused",
    "connection reset",
    "too many connections",
    "deadlock",
    "serialization failure",
}


def _classify_retryable_error(exception: Exception) -> Optional[ErrorClassification]:
    if not get_env_bool("DAL_PROVIDER_AWARE_RETRY", True):
        return None
    provider = (
        get_env_str("QUERY_TARGET_BACKEND") or get_env_str("QUERY_TARGET_PROVIDER") or "postgres"
    )
    return classify_error_info(provider, exception)


def is_transient_error(exception: Exception) -> bool:
    """Check if an exception represents a transient error that is safe to retry.

    Args:
        exception: The exception to check

    Returns:
        True if the error is transient and retryable
    """
    classification = _classify_retryable_error(exception)
    if classification and classification.category != "unknown":
        return classification.is_retryable

    error_msg = str(exception).lower()
    error_type = type(exception).__name__.lower()

    # Check for known transient patterns in error message or type
    for pattern in TRANSIENT_ERROR_PATTERNS:
        if pattern in error_msg or pattern in error_type:
            return True

    # Check for specific exception types
    # asyncpg connection errors
    if "connection" in error_type or "connectionerror" in error_type:
        return True

    return False


async def retry_with_backoff(
    operation: Callable[[], T],
    operation_name: str,
    max_attempts: int = 3,
    base_delay: float = 0.1,
    max_delay: float = 2.0,
    extra_context: dict = None,
) -> T:
    """Retry an async operation with exponential backoff and jitter.

    Args:
        operation: Async callable to retry
        operation_name: Name of operation for logging
        max_attempts: Maximum number of attempts (default: 3)
        base_delay: Initial delay in seconds (default: 0.1)
        max_delay: Maximum delay in seconds (default: 2.0)
        extra_context: Additional context for logging

    Returns:
        Result of the operation if successful

    Raises:
        Exception: The last exception if all retries fail
    """
    extra_context = extra_context or {}
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except Exception as e:
            last_exception = e

            # Log the attempt
            log_extra = {
                "operation": operation_name,
                "attempt": attempt,
                "max_attempts": max_attempts,
                "exception_type": type(e).__name__,
                "exception_message": str(e),
                **extra_context,
            }

            classification = _classify_retryable_error(e)
            span = telemetry.get_current_span()
            if classification:
                log_extra.update(
                    {
                        "retry.classified": True,
                        "retry.error_category": classification.category,
                        "retry.is_retryable": classification.is_retryable,
                        "retry.provider": classification.provider,
                    }
                )
                if span:
                    span.set_attribute("retry.classified", True)
                    span.set_attribute("retry.error_category", classification.category)
                    span.set_attribute("retry.is_retryable", classification.is_retryable)
                    span.set_attribute("retry.provider", classification.provider)
            elif span:
                span.set_attribute("retry.classified", False)

            # Check if error is transient
            if classification and classification.category != "unknown":
                is_retryable = classification.is_retryable
            else:
                is_retryable = is_transient_error(e)

            if not is_retryable:
                logger.error(
                    f"Non-transient error in {operation_name}, not retrying",
                    extra=log_extra,
                    exc_info=True,
                )
                raise

            # On last attempt, don't retry
            if attempt >= max_attempts:
                logger.error(
                    f"All {max_attempts} attempts exhausted for {operation_name}",
                    extra=log_extra,
                    exc_info=True,
                )
                raise

            # Calculate delay with exponential backoff and jitter
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.5)
            total_delay = delay + jitter

            logger.warning(
                f"Transient error in {operation_name}, retrying in {total_delay:.3f}s",
                extra={**log_extra, "delay_seconds": total_delay},
            )

            await asyncio.sleep(total_delay)

    # Should not reach here, but raise last exception just in case
    raise last_exception

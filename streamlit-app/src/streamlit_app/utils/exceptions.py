"""Exception handling utilities for Streamlit app.

Provides helpers for unwrapping ExceptionGroups, particularly those from
asyncio TaskGroups that contain a single root-cause exception.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _is_exception_group(exc: BaseException) -> bool:
    """Check if exception is an ExceptionGroup type (Python 3.11+ compatible).

    Args:
        exc: The exception to check.

    Returns:
        True if exc is an ExceptionGroup or BaseExceptionGroup.
    """
    # BaseExceptionGroup is the base class for ExceptionGroup in Python 3.11+
    # Check by name for flake8 compatibility since it's a builtin
    exc_type_name = type(exc).__name__
    return exc_type_name in ("ExceptionGroup", "BaseExceptionGroup")


def is_single_exception_group(exc: BaseException) -> bool:
    """Check if exception is an ExceptionGroup with exactly one sub-exception.

    Args:
        exc: The exception to check.

    Returns:
        True if exc is an ExceptionGroup/BaseExceptionGroup with exactly one
        contained exception.
    """
    if _is_exception_group(exc) and hasattr(exc, "exceptions"):
        return len(exc.exceptions) == 1  # type: ignore[union-attr]
    return False


def unwrap_single_exception_group(exc: BaseException) -> BaseException:
    """Recursively unwrap ExceptionGroups that contain a single exception.

    If the exception is an ExceptionGroup/BaseExceptionGroup with exactly one
    contained exception, this function recursively unwraps it to return the
    innermost single exception. This is useful for surfacing meaningful error
    messages from asyncio TaskGroups that failed with a single root cause.

    Args:
        exc: The exception to potentially unwrap.

    Returns:
        The innermost single exception if the input was a chain of single-exception
        groups, otherwise the original exception unchanged.
    """
    current = exc

    while is_single_exception_group(current):
        # Safe to access since is_single_exception_group verified it's a group
        inner = current.exceptions[0]  # type: ignore[union-attr]
        logger.debug(
            "Unwrapping single-exception group: %s -> %s",
            type(current).__name__,
            type(inner).__name__,
        )
        current = inner

    return current

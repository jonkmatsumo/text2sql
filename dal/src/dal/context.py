"""Thread-safe tenant context management using contextvars.

This module provides a thread-safe way to manage tenant context across
async operations. Uses Python's contextvars which properly propagate
through async/await boundaries.

Note: For thread-based executors (run_in_executor), see dal/concurrency.py
which will be implemented in Phase 3 to provide ContextAwareExecutor.
"""

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Generator, Optional

# Thread-safe context variable for tenant isolation
# Default is None (no tenant context set)
_tenant_context: ContextVar[Optional[int]] = ContextVar("tenant_id", default=None)


def get_current_tenant() -> Optional[int]:
    """Get the current tenant ID from context.

    Returns:
        The current tenant ID, or None if not set.
    """
    return _tenant_context.get()


def set_current_tenant(tenant_id: Optional[int]) -> Token[Optional[int]]:
    """Set the current tenant ID in context.

    Args:
        tenant_id: The tenant ID to set, or None to clear.

    Returns:
        A token that can be used to reset the context to its previous value.
    """
    return _tenant_context.set(tenant_id)


def reset_tenant_context(token: Token[Optional[int]]) -> None:
    """Reset the tenant context to a previous value.

    Args:
        token: The token returned by set_current_tenant().
    """
    _tenant_context.reset(token)


@contextmanager
def tenant_context(tenant_id: int) -> Generator[int, None, None]:
    """Context manager for scoped tenant context.

    Automatically sets and clears tenant context when entering/exiting.

    Args:
        tenant_id: The tenant ID to set for this scope.

    Yields:
        The tenant ID that was set.

    Example:
        with tenant_context(42):
            # All operations in this block see tenant_id=42
            result = await some_operation()
        # tenant_id is automatically cleared/reset here
    """
    token = set_current_tenant(tenant_id)
    try:
        yield tenant_id
    finally:
        reset_tenant_context(token)

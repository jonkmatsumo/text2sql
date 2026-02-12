"""Request-scoped auth context for MCP middleware/tool coordination."""

from contextvars import ContextVar, Token

_internal_auth_verified: ContextVar[bool] = ContextVar("mcp_internal_auth_verified", default=False)


def is_internal_auth_verified() -> bool:
    """Return whether the current request passed internal token auth."""
    return bool(_internal_auth_verified.get())


def set_internal_auth_verified(verified: bool) -> Token[bool]:
    """Set request-scoped internal auth verification state."""
    return _internal_auth_verified.set(bool(verified))


def reset_internal_auth_verified(token: Token[bool]) -> None:
    """Reset request-scoped internal auth state."""
    _internal_auth_verified.reset(token)

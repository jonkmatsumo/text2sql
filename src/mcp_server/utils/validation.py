"""Shared input validation guards for MCP tool handlers.

Provides copy-paste-safe validation patterns that tools can adopt to
ensure consistent, early rejection of invalid inputs.
"""

from typing import Optional

from mcp_server.utils.errors import tool_error_response


def require_tenant_id(tenant_id: Optional[int], tool_name: str) -> Optional[str]:
    """Validate that a tenant_id is present.

    Returns an error response string if invalid, or None if valid.
    """
    if tenant_id is None:
        return tool_error_response(
            message=f"Tenant ID is required for {tool_name}.",
            code="MISSING_TENANT_ID",
            category="invalid_request",
        )
    return None


def validate_limit(
    limit: int,
    tool_name: str,
    *,
    min_val: int = 1,
    max_val: int = 100,
) -> Optional[str]:
    """Validate that a limit parameter is within bounds.

    Returns an error response string if invalid, or None if valid.
    """
    if not isinstance(limit, int) or limit < min_val or limit > max_val:
        return tool_error_response(
            message=f"Invalid limit for {tool_name}. Must be between {min_val} and {max_val}.",
            code="INVALID_LIMIT",
            category="invalid_request",
        )
    return None


def require_non_empty(value: Optional[str], param_name: str, tool_name: str) -> Optional[str]:
    """Validate that a string parameter is non-empty.

    Returns an error response string if invalid, or None if valid.
    """
    if not value or not value.strip():
        return tool_error_response(
            message=f"Parameter '{param_name}' must be non-empty for {tool_name}.",
            code="EMPTY_PARAMETER",
            category="invalid_request",
        )
    return None

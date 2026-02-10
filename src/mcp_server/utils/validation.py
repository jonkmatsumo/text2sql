"""Shared input validation guards for MCP tool handlers.

Provides copy-paste-safe validation patterns that tools can adopt to
ensure consistent, early rejection of invalid inputs.
"""

from collections.abc import Sequence
from typing import Optional

from mcp_server.utils.errors import tool_error_response

DEFAULT_MAX_INPUT_BYTES = 10 * 1024
DEFAULT_MAX_LIST_ITEMS = 100


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


def validate_string_length(
    value: str,
    *,
    max_bytes: int,
    param_name: str = "value",
    tool_name: str = "tool",
) -> Optional[str]:
    """Validate byte-length of a string parameter.

    Returns an error response string if invalid, or None if valid.
    """
    if not isinstance(value, str):
        return tool_error_response(
            message=f"Parameter '{param_name}' must be a string for {tool_name}.",
            code="INVALID_PARAMETER_TYPE",
            category="invalid_request",
        )

    size = len(value.encode("utf-8"))
    if size > max_bytes:
        return tool_error_response(
            message=(
                f"Parameter '{param_name}' exceeds maximum size of "
                f"{max_bytes} bytes for {tool_name}."
            ),
            code="INPUT_TOO_LARGE",
            category="invalid_request",
        )
    return None


def validate_string_list_length(
    values: Sequence[str],
    *,
    max_items: int,
    param_name: str = "values",
    tool_name: str = "tool",
) -> Optional[str]:
    """Validate item-count and type for a sequence of strings.

    Returns an error response string if invalid, or None if valid.
    """
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return tool_error_response(
            message=f"Parameter '{param_name}' must be a list of strings for {tool_name}.",
            code="INVALID_PARAMETER_TYPE",
            category="invalid_request",
        )
    if len(values) > max_items:
        return tool_error_response(
            message=(
                f"Parameter '{param_name}' exceeds maximum item count of "
                f"{max_items} for {tool_name}."
            ),
            code="TOO_MANY_ITEMS",
            category="invalid_request",
        )
    for index, item in enumerate(values):
        if not isinstance(item, str):
            return tool_error_response(
                message=(
                    f"Parameter '{param_name}' must contain only strings for {tool_name}. "
                    f"Invalid item at index {index}."
                ),
                code="INVALID_PARAMETER_TYPE",
                category="invalid_request",
            )
    return None

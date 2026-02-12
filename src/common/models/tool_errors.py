"""Canonical tool error constructors."""

from __future__ import annotations

from typing import Any, Optional

from common.models.error_metadata import ToolError


def _build_tool_error(
    *,
    category: str,
    code: str,
    message: str,
    retryable: bool = False,
    reason_code: Optional[str] = None,
    provider: Optional[str] = "unknown",
    details_safe: Optional[dict[str, Any]] = None,
    details_debug: Optional[dict[str, Any]] = None,
    retry_after_seconds: Optional[float] = None,
) -> ToolError:
    return ToolError(
        category=category,
        code=code,
        message=message,
        retryable=retryable,
        reason_code=reason_code,
        provider=provider or "unknown",
        details_safe=details_safe,
        details_debug=details_debug,
        retry_after_seconds=retry_after_seconds,
    )


def tool_error_invalid_request(
    *,
    code: str,
    message: str,
    reason_code: Optional[str] = None,
    provider: Optional[str] = "unknown",
    details_safe: Optional[dict[str, Any]] = None,
    details_debug: Optional[dict[str, Any]] = None,
) -> ToolError:
    """Build a canonical invalid-request tool error."""
    return _build_tool_error(
        category="invalid_request",
        code=code,
        message=message,
        retryable=False,
        reason_code=reason_code,
        provider=provider,
        details_safe=details_safe,
        details_debug=details_debug,
    )


def tool_error_unsupported_capability(
    *,
    code: str = "UNSUPPORTED_CAPABILITY",
    message: str,
    reason_code: Optional[str] = "unsupported_capability",
    provider: Optional[str] = "unknown",
    details_safe: Optional[dict[str, Any]] = None,
    details_debug: Optional[dict[str, Any]] = None,
) -> ToolError:
    """Build a canonical unsupported-capability tool error."""
    return _build_tool_error(
        category="unsupported_capability",
        code=code,
        message=message,
        retryable=False,
        reason_code=reason_code,
        provider=provider,
        details_safe=details_safe,
        details_debug=details_debug,
    )


def tool_error_internal(
    *,
    code: str = "INTERNAL_ERROR",
    message: str = "Internal error.",
    reason_code: Optional[str] = "internal_error",
    provider: Optional[str] = "unknown",
    details_safe: Optional[dict[str, Any]] = None,
    details_debug: Optional[dict[str, Any]] = None,
) -> ToolError:
    """Build a canonical internal tool error."""
    return _build_tool_error(
        category="internal",
        code=code,
        message=message,
        retryable=False,
        reason_code=reason_code,
        provider=provider,
        details_safe=details_safe,
        details_debug=details_debug,
    )


def tool_error_timeout(
    *,
    code: str = "TIMEOUT",
    message: str = "Request timed out.",
    reason_code: Optional[str] = "timeout",
    provider: Optional[str] = "unknown",
    details_safe: Optional[dict[str, Any]] = None,
    details_debug: Optional[dict[str, Any]] = None,
    retry_after_seconds: Optional[float] = None,
) -> ToolError:
    """Build a canonical timeout tool error."""
    return _build_tool_error(
        category="timeout",
        code=code,
        message=message,
        retryable=True,
        reason_code=reason_code,
        provider=provider,
        details_safe=details_safe,
        details_debug=details_debug,
        retry_after_seconds=retry_after_seconds,
    )

"""Shared error construction helpers for MCP tool handlers.

Provides a consistent error envelope so the agent never has to special-case
error parsing across different tools.
"""

from typing import Optional

from common.models.error_metadata import ToolError
from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
from common.models.tool_errors import (
    tool_error_internal,
    tool_error_invalid_request,
    tool_error_timeout,
    tool_error_unsupported_capability,
)
from common.sanitization.text import redact_sensitive_info

MAX_ERROR_MESSAGE_LENGTH = 2048


def sanitize_error_message(message: str, fallback: str = "Request failed.") -> str:
    """Redact and bound user-facing error text."""
    safe_text = redact_sensitive_info((message or "").strip())
    if not safe_text:
        safe_text = fallback
    return safe_text[:MAX_ERROR_MESSAGE_LENGTH]


def build_error_metadata(
    *,
    message: str,
    category: str,
    provider: str,
    retryable: bool = False,
    retry_after_seconds: Optional[float] = None,
    code: Optional[str] = None,
    hint: Optional[str] = None,
) -> ToolError:
    """Build bounded, redacted ToolError."""
    safe_message = sanitize_error_message(message)
    safe_hint = sanitize_error_message(hint, fallback="") if hint else None
    if safe_hint == "":
        safe_hint = None
    normalized_category = (category or "internal_error").strip().lower()
    error_code = code or "TOOL_ERROR"
    safe_details = {"hint": safe_hint} if safe_hint else None

    if normalized_category == "invalid_request":
        err = tool_error_invalid_request(
            code=error_code,
            message=safe_message,
            provider=provider,
            details_safe=safe_details,
        )
    elif normalized_category == "unsupported_capability":
        err = tool_error_unsupported_capability(
            code=error_code,
            message=safe_message,
            provider=provider,
            details_safe=safe_details,
        )
    elif normalized_category == "timeout":
        err = tool_error_timeout(
            code=error_code,
            message=safe_message,
            provider=provider,
            details_safe=safe_details,
            retry_after_seconds=retry_after_seconds,
        )
    elif normalized_category in {"internal_error", "unknown"}:
        err = tool_error_internal(
            code=error_code,
            message=safe_message,
            provider=provider,
            details_safe=safe_details,
        )
    else:
        err = ToolError(
            category=normalized_category,
            code=error_code,
            message=safe_message,
            retryable=retryable,
            provider=provider,
            details_safe=safe_details,
            retry_after_seconds=retry_after_seconds,
        )

    updates = {}
    if bool(retryable) != bool(err.retryable):
        updates["retryable"] = bool(retryable)
    if retry_after_seconds is not None and err.retry_after_seconds != retry_after_seconds:
        updates["retry_after_seconds"] = retry_after_seconds
    if safe_hint:
        updates["hint"] = safe_hint
    if updates:
        err = err.model_copy(update=updates)

    return err


def tool_error_response(
    *,
    message: str,
    code: str,
    category: str = "invalid_request",
    provider: str = "mcp_server",
    retryable: bool = False,
    retry_after_seconds: Optional[float] = None,
) -> str:
    """Construct a structured JSON error response for an MCP tool.

    Returns a ToolResponseEnvelope JSON string with a populated ``error``
    field, ensuring the agent receives a uniform error shape regardless
    of which tool emitted it.

    Args:
        message: Human-readable error description (max 2048 chars).
        code: Machine-readable error code (e.g. "MISSING_TENANT_ID").
        category: Provider-agnostic error category.
        provider: Originating service / provider name.
        retryable: Whether the caller should retry.
        retry_after_seconds: Optional backoff hint.
    """
    envelope = ToolResponseEnvelope(
        result=None,
        metadata=GenericToolMetadata(provider=provider),
        error=build_error_metadata(
            message=message,
            category=category,
            provider=provider,
            retryable=retryable,
            retry_after_seconds=retry_after_seconds,
            code=code,
        ),
    )
    return envelope.model_dump_json(exclude_none=True)

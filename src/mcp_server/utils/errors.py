"""Shared error construction helpers for MCP tool handlers.

Provides a consistent error envelope so the agent never has to special-case
error parsing across different tools.
"""

from typing import Optional

from common.errors.error_codes import ErrorCode, canonical_error_code_for_category
from common.models.error_metadata import ErrorCategory, ToolError
from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
from common.models.tool_errors import (
    tool_error_internal,
    tool_error_invalid_request,
    tool_error_timeout,
    tool_error_unsupported_capability,
)
from common.sanitization.text import redact_sensitive_info
from mcp_server.utils.provider import resolve_provider

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
    category: ErrorCategory,
    provider: str | None,
    retryable: bool = False,
    retry_after_seconds: Optional[float] = None,
    code: Optional[str] = None,
    error_code: Optional[str] = None,
    hint: Optional[str] = None,
) -> ToolError:
    """Build bounded, redacted ToolError."""
    resolved_provider = resolve_provider(provider)
    safe_message = sanitize_error_message(message)
    safe_hint = sanitize_error_message(hint, fallback="") if hint else None
    if safe_hint == "":
        safe_hint = None
    machine_code = code or "TOOL_ERROR"
    canonical_error_code = error_code or canonical_error_code_for_category(category).value
    if not canonical_error_code:
        canonical_error_code = ErrorCode.INTERNAL_ERROR.value
    safe_details = {"hint": safe_hint} if safe_hint else None

    if category == ErrorCategory.INVALID_REQUEST:
        err = tool_error_invalid_request(
            code=machine_code,
            message=safe_message,
            provider=resolved_provider,
            details_safe=safe_details,
        )
    elif category == ErrorCategory.UNSUPPORTED_CAPABILITY:
        err = tool_error_unsupported_capability(
            code=machine_code,
            message=safe_message,
            provider=resolved_provider,
            details_safe=safe_details,
        )
    elif category == ErrorCategory.TIMEOUT:
        err = tool_error_timeout(
            code=machine_code,
            message=safe_message,
            provider=resolved_provider,
            details_safe=safe_details,
            retry_after_seconds=retry_after_seconds,
        )
    elif category == ErrorCategory.INTERNAL:
        err = tool_error_internal(
            code=machine_code,
            message=safe_message,
            provider=resolved_provider,
            details_safe=safe_details,
        )
    else:
        err = ToolError(
            category=category,
            code=machine_code,
            error_code=canonical_error_code,
            message=safe_message,
            retryable=retryable,
            provider=resolved_provider,
            details_safe=safe_details,
            retry_after_seconds=retry_after_seconds,
        )

    updates = {"error_code": canonical_error_code}
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
    error_code: Optional[str] = None,
    category: ErrorCategory = ErrorCategory.INVALID_REQUEST,
    provider: str | None = None,
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
    resolved_provider = resolve_provider(provider)
    envelope = ToolResponseEnvelope(
        result=None,
        metadata=GenericToolMetadata(provider=resolved_provider),
        error=build_error_metadata(
            message=message,
            category=category,
            provider=resolved_provider,
            retryable=retryable,
            retry_after_seconds=retry_after_seconds,
            code=code,
            error_code=error_code,
        ),
    )
    return envelope.model_dump_json(exclude_none=True)

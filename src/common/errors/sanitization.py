"""Sanitization helpers for user-facing error surfaces."""

from __future__ import annotations

import re
from typing import Any

from common.errors.error_codes import ErrorCode, parse_error_code
from common.sanitization.text import redact_sensitive_info

MAX_PUBLIC_ERROR_LENGTH = 2048

_SAFE_ERROR_TEMPLATES: dict[ErrorCode, str] = {
    ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED: (
        "Tenant isolation is not supported for this provider."
    ),
    ErrorCode.DB_CONNECTION_ERROR: "Database connection failed.",
    ErrorCode.DB_TIMEOUT: "Database connection timed out.",
    ErrorCode.DB_SYNTAX_ERROR: "SQL validation failed.",
    ErrorCode.INTERNAL_ERROR: "An internal error occurred.",
}

_SQL_FRAGMENT_RE = re.compile(
    r"(?is)\b(select|insert|update|delete|merge|create|drop|alter|truncate|call)\b"
    r".*\b(from|into|table|set|values|where)\b"
)
_QUOTED_IDENTIFIER_RE = re.compile(r"[\"'`](?:[^\"'`]|\\.)+[\"'`]")
_DOTTED_IDENTIFIER_RE = re.compile(r"\b[a-zA-Z_][\w$]*\.[a-zA-Z_][\w$]*\b")
_MULTI_SPACE_RE = re.compile(r"\s+")


def _sanitize_sql_like_text(message: str) -> str:
    if _SQL_FRAGMENT_RE.search(message):
        return "SQL validation failed."
    sanitized = _QUOTED_IDENTIFIER_RE.sub("<redacted_identifier>", message)
    sanitized = _DOTTED_IDENTIFIER_RE.sub("<redacted_identifier>", sanitized)
    sanitized = _MULTI_SPACE_RE.sub(" ", sanitized).strip()
    return sanitized


def sanitize_error_message(
    message: Any,
    *,
    error_code: Any = None,
    fallback: str = "Request failed.",
) -> str:
    """Return safe user-facing error text without leaking SQL/identifiers."""
    code = parse_error_code(error_code)
    template = _SAFE_ERROR_TEMPLATES.get(code)
    if template:
        return template

    raw_text = "" if message is None else str(message)
    bounded_fallback = (fallback or "Request failed.").strip()[:MAX_PUBLIC_ERROR_LENGTH]
    safe_text = redact_sensitive_info(raw_text.strip())
    if not safe_text:
        safe_text = bounded_fallback
    safe_text = _sanitize_sql_like_text(safe_text)
    if not safe_text:
        safe_text = bounded_fallback
    return safe_text[:MAX_PUBLIC_ERROR_LENGTH]


def sanitize_exception(
    exc: Exception,
    *,
    error_code: Any = ErrorCode.INTERNAL_ERROR.value,
    fallback: str = "Request failed.",
) -> str:
    """Sanitize an exception for outward-facing API/tool contracts."""
    return sanitize_error_message(
        str(exc),
        error_code=error_code,
        fallback=fallback,
    )

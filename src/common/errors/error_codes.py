"""Canonical error-code taxonomy for DAL/MCP/Agent flows."""

from __future__ import annotations

from enum import Enum
from typing import Any

from common.models.error_metadata import ErrorCategory


class ErrorCode(str, Enum):
    """Bounded canonical error codes for external contracts and observability."""

    VALIDATION_ERROR = "VALIDATION_ERROR"
    TENANT_ENFORCEMENT_UNSUPPORTED = "TENANT_ENFORCEMENT_UNSUPPORTED"
    READONLY_VIOLATION = "READONLY_VIOLATION"
    SQL_POLICY_VIOLATION = "SQL_POLICY_VIOLATION"
    DB_CONNECTION_ERROR = "DB_CONNECTION_ERROR"
    DB_TIMEOUT = "DB_TIMEOUT"
    DB_SYNTAX_ERROR = "DB_SYNTAX_ERROR"
    AMBIGUITY_UNRESOLVED = "AMBIGUITY_UNRESOLVED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


_CATEGORY_TO_CODE: dict[str, ErrorCode] = {
    ErrorCategory.INVALID_REQUEST.value: ErrorCode.VALIDATION_ERROR,
    ErrorCategory.UNSUPPORTED_CAPABILITY.value: ErrorCode.VALIDATION_ERROR,
    ErrorCategory.TOOL_VERSION_INVALID.value: ErrorCode.VALIDATION_ERROR,
    ErrorCategory.TOOL_VERSION_UNSUPPORTED.value: ErrorCode.VALIDATION_ERROR,
    ErrorCategory.TENANT_ENFORCEMENT_UNSUPPORTED.value: ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED,
    ErrorCategory.MUTATION_BLOCKED.value: ErrorCode.READONLY_VIOLATION,
    ErrorCategory.UNAUTHORIZED.value: ErrorCode.SQL_POLICY_VIOLATION,
    ErrorCategory.AUTH.value: ErrorCode.SQL_POLICY_VIOLATION,
    ErrorCategory.SYNTAX.value: ErrorCode.DB_SYNTAX_ERROR,
    ErrorCategory.TIMEOUT.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.CONNECTIVITY.value: ErrorCode.DB_CONNECTION_ERROR,
    ErrorCategory.DEADLOCK.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.SERIALIZATION.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.THROTTLING.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.RESOURCE_EXHAUSTED.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.TRANSIENT.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.SCHEMA_DRIFT.value: ErrorCode.DB_SYNTAX_ERROR,
    ErrorCategory.DEPENDENCY_FAILURE.value: ErrorCode.DB_CONNECTION_ERROR,
    ErrorCategory.LIMIT_EXCEEDED.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.BUDGET_EXCEEDED.value: ErrorCode.DB_TIMEOUT,
    ErrorCategory.UNKNOWN.value: ErrorCode.INTERNAL_ERROR,
    ErrorCategory.INTERNAL.value: ErrorCode.INTERNAL_ERROR,
    ErrorCategory.TOOL_RESPONSE_MALFORMED.value: ErrorCode.INTERNAL_ERROR,
}

_CODE_GROUPS: dict[ErrorCode, str] = {
    ErrorCode.VALIDATION_ERROR: "VALIDATION",
    ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED: "TENANT",
    ErrorCode.READONLY_VIOLATION: "POLICY",
    ErrorCode.SQL_POLICY_VIOLATION: "POLICY",
    ErrorCode.DB_CONNECTION_ERROR: "DB",
    ErrorCode.DB_TIMEOUT: "DB",
    ErrorCode.DB_SYNTAX_ERROR: "DB",
    ErrorCode.AMBIGUITY_UNRESOLVED: "AMBIGUITY",
    ErrorCode.INTERNAL_ERROR: "INTERNAL",
}


def _normalize_category(category: str | ErrorCategory | None) -> str:
    if isinstance(category, ErrorCategory):
        return category.value
    if category is None:
        return ""
    return str(category).strip()


def canonical_error_code_for_category(
    category: str | ErrorCategory | None,
    *,
    fallback: ErrorCode = ErrorCode.INTERNAL_ERROR,
) -> ErrorCode:
    """Resolve canonical error code from category-like values."""
    normalized = _normalize_category(category)
    if not normalized:
        return fallback
    return _CATEGORY_TO_CODE.get(normalized, fallback)


def parse_error_code(
    value: Any,
    *,
    fallback: ErrorCode = ErrorCode.INTERNAL_ERROR,
) -> ErrorCode:
    """Parse string-like values to `ErrorCode` with safe fallback."""
    if isinstance(value, ErrorCode):
        return value
    if value is None:
        return fallback
    try:
        return ErrorCode(str(value).strip())
    except Exception:
        return fallback


def error_code_group(value: Any) -> str:
    """Return a stable coarse grouping for telemetry dimensions."""
    parsed = parse_error_code(value)
    return _CODE_GROUPS.get(parsed, "INTERNAL")

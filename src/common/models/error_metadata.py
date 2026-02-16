"""Structured error metadata models."""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


class ErrorCategory(str, Enum):
    """Canonical error categories."""

    AUTH = "auth"
    LIMIT_EXCEEDED = "limit_exceeded"
    INVALID_REQUEST = "invalid_request"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    TIMEOUT = "timeout"
    SCHEMA_DRIFT = "schema_drift"
    INTERNAL = "internal"
    CONNECTIVITY = "connectivity"
    SYNTAX = "syntax"
    DEADLOCK = "deadlock"
    SERIALIZATION = "serialization"
    THROTTLING = "throttling"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    BUDGET_EXCEEDED = "budget_exceeded"
    TRANSIENT = "transient"
    UNKNOWN = "unknown"

    # MCP / Extension categories
    DEPENDENCY_FAILURE = "dependency_failure"
    MUTATION_BLOCKED = "mutation_blocked"
    UNAUTHORIZED = "unauthorized"
    TOOL_VERSION_INVALID = "tool_version_invalid"
    TOOL_VERSION_UNSUPPORTED = "tool_version_unsupported"
    TOOL_RESPONSE_MALFORMED = "tool_response_malformed"


class ToolError(BaseModel):
    """Canonical tool error contract.

    Legacy aliases are accepted and emitted for compatibility:
    - code <-> sql_state
    - retryable <-> is_retryable
    - reason_code <-> retry_reason
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    category: ErrorCategory = Field(..., description="Provider-agnostic error category")
    code: Optional[str] = Field(None, description="Stable machine-readable error code")
    message: str = Field(
        ..., max_length=2048, description="Safe user-facing error message (redacted/bounded)"
    )
    retryable: bool = Field(False, description="Whether the error is retryable")
    reason_code: Optional[str] = Field(None, description="Stable retry/decision reason code")
    details_safe: Optional[dict[str, Any]] = Field(
        None, description="Safe details that can be surfaced to users/agent"
    )
    details_debug: Optional[dict[str, Any]] = Field(
        None, description="Debug-only details; must not be surfaced to end users"
    )
    provider: Optional[str] = Field("unknown", description="Originating provider/system")
    retry_after_seconds: Optional[float] = Field(
        None, description="Suggested delay before retrying"
    )
    line_number: Optional[int] = Field(None, description="Line number where the error occurred")
    position: Optional[int] = Field(None, description="Character position where the error occurred")
    hint: Optional[str] = Field(
        None, max_length=2048, description="Provider-specific hint or suggestion"
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_fields(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        data = dict(value)
        if "code" not in data and "sql_state" in data:
            data["code"] = data.get("sql_state")
        if "retryable" not in data and "is_retryable" in data:
            data["retryable"] = data.get("is_retryable")
        if "reason_code" not in data and "retry_reason" in data:
            data["reason_code"] = data.get("retry_reason")
        return data

    @model_serializer(mode="wrap")
    def _serialize_with_legacy(self, handler):
        payload = handler(self)
        # Keep compatibility with legacy clients/tests while exposing canonical fields.
        payload["sql_state"] = payload.get("code")
        payload["is_retryable"] = bool(payload.get("retryable", False))
        if payload.get("reason_code") is not None:
            payload["retry_reason"] = payload.get("reason_code")
        return payload

    @property
    def sql_state(self) -> Optional[str]:
        """Legacy alias for code."""
        return self.code

    @property
    def is_retryable(self) -> bool:
        """Legacy alias for retryable."""
        return self.retryable

    @property
    def retry_reason(self) -> Optional[str]:
        """Legacy alias for reason_code."""
        return self.reason_code

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses/telemetry."""
        return self.model_dump(exclude_none=True)


class ErrorMetadata(ToolError):
    """Backward-compatible alias for ToolError."""

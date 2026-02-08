"""Typed envelope models for tool IO."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from common.models.error_metadata import ErrorMetadata

# Current schema version for future-proofing
CURRENT_SCHEMA_VERSION = "1.0"


class ExecuteSQLQueryMetadata(BaseModel):
    """Metadata for SQL query execution results."""

    rows_returned: int = Field(..., description="Number of rows in the current page")
    is_truncated: bool = Field(False, description="Whether the result was truncated")
    is_limited: bool = Field(False, description="Whether the result was limited by LIMIT clause")
    is_paginated: bool = Field(False, description="Whether the result is part of a paginated set")
    partial_reason: Optional[str] = Field(
        None, description="Reason for partial results (e.g. MAX_ROWS, SIZE_LIMIT)"
    )
    next_page_token: Optional[str] = Field(None, description="Token for fetching the next page")
    bytes_returned: Optional[int] = Field(
        None, description="Estimated size of the payload in bytes"
    )

    # Capability negotiation fields
    capability_required: Optional[str] = None
    capability_supported: Optional[bool] = None
    fallback_policy: Optional[str] = None
    fallback_applied: Optional[bool] = None
    fallback_mode: Optional[str] = None

    # Provider limits
    row_limit: Optional[int] = None

    # Cap mitigation
    cap_detected: bool = False
    cap_mitigation_applied: bool = False
    cap_mitigation_mode: Optional[str] = None


class ExecuteSQLQueryResponseEnvelope(BaseModel):
    """Standardized envelope for execute_sql_query tool responses."""

    schema_version: str = Field(default=CURRENT_SCHEMA_VERSION)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    columns: Optional[List[Dict[str, Any]]] = None
    metadata: ExecuteSQLQueryMetadata
    error: Optional[ErrorMetadata] = None

    # Optional raw error message for simple cases (legacy support)
    error_message: Optional[str] = None

    @field_validator("rows")
    def validate_rows(cls, v):
        """Ensure rows is always a list."""
        if v is None:
            return []
        return v

    def is_error(self) -> bool:
        """Check if the envelope represents an error."""
        return self.error is not None or self.error_message is not None


def is_compatible(payload_version: str, current_version: str) -> bool:
    """Check if payload_version is compatible with current_version.

    Rules:
    - Major versions must match exactly.
    - We support both backward and forward compatibility for minor versions
      within the same major version.
    """
    try:
        p_parts = payload_version.split(".")
        c_parts = current_version.split(".")

        p_major = int(p_parts[0])
        c_major = int(c_parts[0])

        if p_major != c_major:
            return False

        # Within the same major version, we assume compatibility.
        # Future work could add stricter checks for specific minor version features.
        return True
    except (ValueError, IndexError):
        return False


def parse_execute_sql_response(payload: Any) -> ExecuteSQLQueryResponseEnvelope:
    """Parse a raw payload into a typed envelope.

    Handles:
    - Already typed objects
    - Dictionary representations
    - JSON strings (decoding them first)
    """
    import json

    if isinstance(payload, ExecuteSQLQueryResponseEnvelope):
        return payload

    raw_data = payload
    if isinstance(payload, str):
        try:
            raw_data = json.loads(payload)
        except json.JSONDecodeError:
            # Fallback for plain string errors (legacy)
            return _create_error_envelope(payload)

    if not isinstance(raw_data, dict):
        return _create_error_envelope(f"Invalid payload type: {type(raw_data)}")

    # Check if it matches the envelope structure (duck typing)
    if "metadata" in raw_data and "rows" in raw_data:
        try:
            envelope = ExecuteSQLQueryResponseEnvelope.model_validate(raw_data)

            # Version Migration / Compatibility Check
            payload_version = envelope.schema_version or "1.0"
            if not is_compatible(payload_version, CURRENT_SCHEMA_VERSION):
                return _create_error_envelope(
                    f"Incompatible envelope version: {payload_version} "
                    f"(supported: {CURRENT_SCHEMA_VERSION.split('.')[0]}.x)",
                    category="invalid_response_version",
                )

            return envelope
        except Exception:
            # Fall through to legacy/malformed handling
            pass

    # Legacy/Partial formats handling would go here or in the caller
    # For this strict parser, we expect the envelope structure or we try to adapt basic errors

    if "error" in raw_data and isinstance(raw_data["error"], str):
        # Adapt simple error dict
        return _create_error_envelope(
            raw_data["error"],
            category=raw_data.get("error_category"),
            metadata=raw_data.get("error_metadata"),
        )

    # If we really can't parse it, return an error envelope
    return _create_error_envelope("Malformed response payload")


class GenericToolMetadata(BaseModel):
    """Generic metadata for tool responses."""

    provider: str = Field("unknown", description="Database or system provider")
    execution_time_ms: Optional[float] = None


class GenericToolResponseEnvelope(BaseModel):
    """Standardized envelope for miscellaneous tool responses."""

    schema_version: str = Field(default=CURRENT_SCHEMA_VERSION)
    result: Any = Field(..., description="The tool's main output payload")
    metadata: GenericToolMetadata = Field(default_factory=GenericToolMetadata)
    error: Optional[ErrorMetadata] = None

    def model_dump_json(self, **kwargs) -> str:
        """Dump model to JSON string."""
        return super().model_dump_json(**kwargs)


class ToolResponseEnvelope(GenericToolResponseEnvelope):
    """Alias for GenericToolResponseEnvelope for backward compatibility/clarity."""

    pass


def _create_error_envelope(
    message: str, category: str = "unknown", metadata: Optional[Dict[str, Any]] = None
) -> ExecuteSQLQueryResponseEnvelope:
    """Create an error envelope helper."""
    error_meta = ErrorMetadata(
        message=message, category=category or "unknown", provider="unknown", is_retryable=False
    )
    if metadata:
        # Best effort merge of extra metadata
        pass

    return ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=ExecuteSQLQueryMetadata(rows_returned=0, is_truncated=False),
        error=error_meta,
        error_message=message,
    )

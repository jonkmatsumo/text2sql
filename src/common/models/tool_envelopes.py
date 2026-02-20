"""Typed envelope models for tool IO."""

from typing import Any, Dict, Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field, field_validator, model_validator

from common.models.error_metadata import ToolError
from common.models.tool_versions import DEFAULT_TOOL_VERSION

# Current schema version for future-proofing
CURRENT_SCHEMA_VERSION = "1.0"
CURRENT_TOOL_VERSION = DEFAULT_TOOL_VERSION
T = TypeVar("T")


class ExecuteSQLQueryMetadata(BaseModel):
    """Metadata for SQL query execution results."""

    tool_version: str = Field(
        default=CURRENT_TOOL_VERSION,
        description="Semantic version for execute_sql_query response contract",
    )
    rows_returned: int = Field(..., description="Number of rows in the current page")
    is_truncated: bool = Field(False, description="Whether the result was truncated")
    truncated: Optional[bool] = Field(
        None, description="Standardized truncation flag alias for is_truncated"
    )
    is_limited: bool = Field(False, description="Whether the result was limited by LIMIT clause")
    is_paginated: bool = Field(False, description="Whether the result is part of a paginated set")
    partial_reason: Optional[str] = Field(
        None, description="Reason for partial results (e.g. MAX_ROWS, SIZE_LIMIT)"
    )
    next_page_token: Optional[str] = Field(None, description="Token for fetching the next page")
    next_cursor: Optional[str] = Field(
        None, description="Standardized pagination cursor alias for next_page_token"
    )
    returned_count: Optional[int] = Field(
        None, description="Standardized row-count alias for rows_returned"
    )
    limit_applied: Optional[int] = Field(None, description="Standardized limit alias for row_limit")
    bytes_returned: Optional[int] = Field(
        None, description="Estimated size of the payload in bytes"
    )
    truncation_reason: Optional[str] = Field(
        None, description="Standardized truncation reason alias for partial_reason"
    )
    request_id: Optional[str] = Field(
        None, description="Request identifier propagated for cross-layer correlation"
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

    @model_validator(mode="before")
    @classmethod
    def normalize_standardized_fields(cls, data: Any) -> Any:
        """Keep legacy and standardized metadata aliases in sync."""
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        if normalized.get("rows_returned") is None and normalized.get("returned_count") is not None:
            normalized["rows_returned"] = normalized["returned_count"]
        if normalized.get("returned_count") is None and normalized.get("rows_returned") is not None:
            normalized["returned_count"] = normalized["rows_returned"]

        if normalized.get("is_truncated") is None and normalized.get("truncated") is not None:
            normalized["is_truncated"] = normalized["truncated"]
        if normalized.get("truncated") is None and normalized.get("is_truncated") is not None:
            normalized["truncated"] = normalized["is_truncated"]

        if normalized.get("row_limit") is None and normalized.get("limit_applied") is not None:
            normalized["row_limit"] = normalized["limit_applied"]
        if normalized.get("limit_applied") is None and normalized.get("row_limit") is not None:
            normalized["limit_applied"] = normalized["row_limit"]

        if normalized.get("next_page_token") is None and normalized.get("next_cursor") is not None:
            normalized["next_page_token"] = normalized["next_cursor"]
        if normalized.get("next_cursor") is None and normalized.get("next_page_token") is not None:
            normalized["next_cursor"] = normalized["next_page_token"]

        if (
            normalized.get("partial_reason") is None
            and normalized.get("truncation_reason") is not None
        ):
            normalized["partial_reason"] = normalized["truncation_reason"]
        if (
            normalized.get("truncation_reason") is None
            and normalized.get("partial_reason") is not None
        ):
            normalized["truncation_reason"] = normalized["partial_reason"]

        return normalized

    @model_validator(mode="after")
    def sync_standardized_fields(self) -> "ExecuteSQLQueryMetadata":
        """Fill alias fields when defaults are applied after input normalization."""
        if self.truncated is None:
            self.truncated = bool(self.is_truncated)
        if self.returned_count is None:
            self.returned_count = int(self.rows_returned)
        if self.limit_applied is None and self.row_limit is not None:
            self.limit_applied = int(self.row_limit)
        if self.next_cursor is None and self.next_page_token is not None:
            self.next_cursor = self.next_page_token
        if self.next_page_token is None and self.next_cursor is not None:
            self.next_page_token = self.next_cursor
        if self.truncation_reason is None and self.partial_reason is not None:
            self.truncation_reason = self.partial_reason
        return self


class ExecuteSQLQueryResponseEnvelope(BaseModel):
    """Standardized envelope for execute_sql_query tool responses."""

    schema_version: str = Field(default=CURRENT_SCHEMA_VERSION)
    rows: List[Dict[str, Any]] = Field(default_factory=list)
    columns: Optional[List[Dict[str, Any]]] = None
    metadata: ExecuteSQLQueryMetadata
    error: Optional[ToolError] = None

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
            return _create_error_envelope(payload, category="tool_response_malformed")

    if not isinstance(raw_data, dict):
        return _create_error_envelope(
            f"Invalid payload type: {type(raw_data)}", category="tool_response_malformed"
        )

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
                    category="tool_version_unsupported",
                )

            return envelope
        except Exception:
            # Fall through to legacy/malformed handling
            pass

    # Legacy/Partial formats handling would go here or in the caller
    # For this strict parser, we expect the envelope structure or we try to adapt basic errors

    if "error" in raw_data:
        error_val = raw_data["error"]
        msg = error_val if isinstance(error_val, str) else error_val.get("message", "Unknown error")
        category = raw_data.get("error_category")
        metadata = raw_data.get("error_metadata")

        # If error is a dict, it might contain category/metadata
        if isinstance(error_val, dict):
            category = category or error_val.get("category")
            metadata = metadata or error_val.get("metadata")

        return _create_error_envelope(
            msg,
            category=category or "tool_response_malformed",
            metadata=metadata or raw_data.get("error_metadata") or raw_data.get("metadata"),
        )

    # If we really can't parse it, return an error envelope
    return _create_error_envelope("Malformed response payload", category="tool_response_malformed")


class GenericToolMetadata(BaseModel):
    """Generic metadata for tool responses."""

    tool_version: str = Field(
        default=CURRENT_TOOL_VERSION,
        description="Semantic version for tool response contract",
    )
    provider: str = Field("unknown", description="Database or system provider")
    execution_time_ms: Optional[float] = None
    truncated: Optional[bool] = None
    returned_count: Optional[int] = None
    limit_applied: Optional[int] = None
    next_cursor: Optional[str] = None
    next_page_token: Optional[str] = None
    is_truncated: Optional[bool] = None
    truncation_reason: Optional[str] = None
    items_returned: Optional[int] = None
    items_total: Optional[int] = None
    bytes_returned: Optional[int] = None
    bytes_total: Optional[int] = None
    snapshot_id: Optional[str] = Field(None, description="Schema snapshot ID for lineage tracking")
    request_id: Optional[str] = Field(
        None, description="Request identifier propagated for cross-layer correlation"
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_standardized_fields(cls, data: Any) -> Any:
        """Keep generic metadata aliases synchronized."""
        if not isinstance(data, dict):
            return data

        normalized = dict(data)
        if normalized.get("is_truncated") is None and normalized.get("truncated") is not None:
            normalized["is_truncated"] = normalized["truncated"]
        if normalized.get("truncated") is None and normalized.get("is_truncated") is not None:
            normalized["truncated"] = normalized["is_truncated"]

        if (
            normalized.get("items_returned") is None
            and normalized.get("returned_count") is not None
        ):
            normalized["items_returned"] = normalized["returned_count"]
        if (
            normalized.get("returned_count") is None
            and normalized.get("items_returned") is not None
        ):
            normalized["returned_count"] = normalized["items_returned"]

        if normalized.get("next_page_token") is None and normalized.get("next_cursor") is not None:
            normalized["next_page_token"] = normalized["next_cursor"]
        if normalized.get("next_cursor") is None and normalized.get("next_page_token") is not None:
            normalized["next_cursor"] = normalized["next_page_token"]

        if normalized.get("limit_applied") is None and normalized.get("row_limit") is not None:
            normalized["limit_applied"] = normalized["row_limit"]

        return normalized

    @model_validator(mode="after")
    def sync_standardized_fields(self) -> "GenericToolMetadata":
        """Fill standardized fields from legacy defaults when omitted."""
        if self.truncated is None and self.is_truncated is not None:
            self.truncated = bool(self.is_truncated)
        if self.is_truncated is None and self.truncated is not None:
            self.is_truncated = bool(self.truncated)
        if self.returned_count is None and self.items_returned is not None:
            self.returned_count = int(self.items_returned)
        if self.items_returned is None and self.returned_count is not None:
            self.items_returned = int(self.returned_count)
        if self.next_cursor is None and self.next_page_token is not None:
            self.next_cursor = self.next_page_token
        if self.next_page_token is None and self.next_cursor is not None:
            self.next_page_token = self.next_cursor
        return self


class GenericToolResponseEnvelope(BaseModel, Generic[T]):
    """Standardized envelope for miscellaneous tool responses."""

    schema_version: str = Field(default=CURRENT_SCHEMA_VERSION)
    result: Optional[T] = Field(default=None, description="The tool's main output payload")
    metadata: GenericToolMetadata = Field(default_factory=GenericToolMetadata)
    error: Optional[ToolError] = None

    def model_dump_json(self, **kwargs) -> str:
        """Dump model to JSON string."""
        return super().model_dump_json(**kwargs)


class ToolResponseEnvelope(GenericToolResponseEnvelope[T], Generic[T]):
    """Alias for GenericToolResponseEnvelope for backward compatibility/clarity."""

    pass


def _create_error_envelope(
    message: str, category: str = "unknown", metadata: Optional[Dict[str, Any]] = None
) -> ExecuteSQLQueryResponseEnvelope:
    """Create an error envelope helper."""
    error_meta = ToolError(
        category=category or "unknown",
        code="TOOL_ERROR",
        message=message,
        retryable=False,
        provider="unknown",
    )
    meta = ExecuteSQLQueryMetadata(rows_returned=0, is_truncated=False)
    if metadata:
        meta.capability_required = metadata.get("capability_required") or metadata.get(
            "required_capability"
        )
        meta.capability_supported = metadata.get("capability_supported")
        meta.fallback_policy = metadata.get("fallback_policy")
        meta.fallback_applied = metadata.get("fallback_applied")
        meta.fallback_mode = metadata.get("fallback_mode")

    return ExecuteSQLQueryResponseEnvelope(
        rows=[],
        metadata=meta,
        error=error_meta,
        error_message=message,
    )

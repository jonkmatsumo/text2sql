"""Utility functions for tool response envelopes."""

from typing import Any

from common.models.error_metadata import ErrorMetadata
from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope


def tool_success_response(result: Any, provider: str = "unknown") -> str:
    """Construct a standardized success response envelope."""
    envelope = ToolResponseEnvelope(
        result=result,
        metadata=GenericToolMetadata(provider=provider),
    )
    return envelope.model_dump_json(exclude_none=True)


def tool_error_response(
    message: str,
    category: str = "unknown",
    provider: str = "unknown",
    is_retryable: bool = False,
) -> str:
    """Construct a standardized error response envelope."""
    error_meta = ErrorMetadata(
        message=message,
        category=category,
        provider=provider,
        is_retryable=is_retryable,
    )
    envelope = ToolResponseEnvelope(
        result=None,
        error=error_meta,
    )
    return envelope.model_dump_json(exclude_none=True)

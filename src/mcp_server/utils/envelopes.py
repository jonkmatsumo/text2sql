"""Utility functions for tool response envelopes."""

from typing import Any

from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope


def tool_success_response(result: Any, provider: str = "unknown") -> str:
    """Construct a standardized success response envelope."""
    envelope = ToolResponseEnvelope(
        result=result,
        metadata=GenericToolMetadata(provider=provider),
    )
    return envelope.model_dump_json(exclude_none=True)

"""Utility functions for tool response envelopes."""

from typing import Any

from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
from mcp_server.utils.provider import resolve_provider


def tool_success_response(result: Any, provider: str | None = None) -> str:
    """Construct a standardized success response envelope."""
    resolved_provider = resolve_provider(provider)
    envelope = ToolResponseEnvelope(
        result=result,
        metadata=GenericToolMetadata(provider=resolved_provider),
    )
    return envelope.model_dump_json(exclude_none=True)

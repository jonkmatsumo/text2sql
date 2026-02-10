"""Shared error construction helpers for MCP tool handlers.

Provides a consistent error envelope so the agent never has to special-case
error parsing across different tools.
"""

from typing import Optional

from common.models.error_metadata import ErrorMetadata
from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope


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
        error=ErrorMetadata(
            message=message[:2048],
            category=category,
            provider=provider,
            is_retryable=retryable,
            retry_after_seconds=retry_after_seconds,
            sql_state=code,  # reuse sql_state for machine-readable code
        ),
    )
    return envelope.model_dump_json(exclude_none=True)

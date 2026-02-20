"""MCP tool: resolve_ambiguity - Detect and resolve query ambiguity."""

import logging
from typing import Any, Dict, List, Optional

from mcp_server.services.ambiguity.resolver import AmbiguityResolver

TOOL_NAME = "resolve_ambiguity"
TOOL_DESCRIPTION = "Resolve potential ambiguities in a user query against provided schema context."

logger = logging.getLogger(__name__)

# Singleton resolver
_resolver: Optional[AmbiguityResolver] = None


def get_resolver() -> AmbiguityResolver:
    """Get or initialize the singleton resolver."""
    global _resolver
    if _resolver is None:
        _resolver = AmbiguityResolver()
    return _resolver


async def handler(query: str, schema_context: List[Dict[str, Any]]) -> str:
    """Resolve potential ambiguities in a user query against provided schema context.

    Authorization:
        No explicit role gate in this tool. Access control is enforced by upstream
        transport/auth layers.

    Data Access:
        Read-only access to the internal ambiguity resolution logic and provided
        schema context.

    Failure Modes:
        - Resolution Failed: If the resolver encounters internal errors during analysis.

    Args:
        query: The user query to analyze for ambiguities.
        schema_context: List of schema objects providing context for resolution.

    Returns:
        JSON string with resolution status and bindings.
    """
    import time

    from mcp_server.utils.errors import tool_error_response
    from mcp_server.utils.validation import (
        DEFAULT_MAX_INPUT_BYTES,
        DEFAULT_MAX_LIST_ITEMS,
        validate_string_length,
        validate_string_list_length,
    )

    if err := validate_string_length(
        query,
        max_bytes=DEFAULT_MAX_INPUT_BYTES,
        param_name="query",
        tool_name=TOOL_NAME,
    ):
        return err

    if not isinstance(schema_context, list):
        return tool_error_response(
            message=f"Parameter 'schema_context' must be a list for {TOOL_NAME}.",
            code="INVALID_PARAMETER_TYPE",
            category="invalid_request",
        )

    schema_names = []
    for item in schema_context:
        if isinstance(item, dict):
            name = item.get("name")
            if isinstance(name, str) and name:
                schema_names.append(name)
    if err := validate_string_list_length(
        schema_names,
        max_items=DEFAULT_MAX_LIST_ITEMS,
        param_name="schema_context",
        tool_name=TOOL_NAME,
    ):
        return err

    start_time = time.monotonic()

    try:
        resolver = get_resolver()
        result = resolver.resolve(query, schema_context)

        execution_time_ms = (time.monotonic() - start_time) * 1000

        from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

        envelope = ToolResponseEnvelope(
            result=result,
            metadata=GenericToolMetadata(
                provider="ambiguity_resolver", execution_time_ms=execution_time_ms
            ),
        )
        return envelope.model_dump_json(exclude_none=True)
    except Exception as e:
        logger.error(f"Ambiguity resolution failed: {e}")
        # Build a manual error envelope
        from common.models.tool_envelopes import ToolResponseEnvelope
        from mcp_server.utils.errors import build_error_metadata

        envelope = ToolResponseEnvelope(
            result={"status": "ERROR", "resolved_bindings": {}, "ambiguities": []},
            error=build_error_metadata(
                message="Ambiguity resolution failed.",
                category="ambiguity_resolution_failed",
                provider="ambiguity_resolver",
                retryable=False,
                code="AMBIGUITY_RESOLUTION_FAILED",
            ),
        )
        return envelope.model_dump_json(exclude_none=True)

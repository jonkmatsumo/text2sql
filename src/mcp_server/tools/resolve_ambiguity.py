"""MCP tool: resolve_ambiguity - Detect and resolve query ambiguity."""

import logging
from typing import Any, Dict, List, Optional

from mcp_server.services.ambiguity.resolver import AmbiguityResolver

TOOL_NAME = "resolve_ambiguity"

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

    Args:
        query: The user query to analyze for ambiguities.
        schema_context: List of schema objects providing context for resolution.

    Returns:
        JSON string with resolution status and bindings.
    """
    import time

    start_time = time.monotonic()

    try:
        resolver = get_resolver()
        result = resolver.resolve(query, schema_context)

        execution_time_ms = (time.monotonic() - start_time) * 1000

        from common.models.tool_envelopes import GenericToolMetadata, GenericToolResponseEnvelope

        envelope = GenericToolResponseEnvelope(
            result=result,
            metadata=GenericToolMetadata(
                provider="ambiguity_resolver", execution_time_ms=execution_time_ms
            ),
        )
        return envelope.model_dump_json(exclude_none=True)
    except Exception as e:
        logger.error(f"Ambiguity resolution failed: {e}")
        # Build a manual error envelope
        from common.models.error_metadata import ErrorMetadata
        from common.models.tool_envelopes import GenericToolResponseEnvelope

        return GenericToolResponseEnvelope(
            result={"status": "ERROR", "resolved_bindings": {}, "ambiguities": []},
            error=ErrorMetadata(
                message=str(e),
                category="ambiguity_resolution_failed",
                provider="ambiguity_resolver",
            ),
        ).model_dump_json(exclude_none=True)

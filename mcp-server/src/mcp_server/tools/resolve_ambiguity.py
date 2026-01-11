"""MCP tool: resolve_ambiguity - Detect and resolve query ambiguity."""

import json
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
    try:
        resolver = get_resolver()
        result = resolver.resolve(query, schema_context)
        return json.dumps(result, separators=(",", ":"))
    except Exception as e:
        logger.error(f"Ambiguity resolution failed: {e}")
        return json.dumps(
            {"status": "ERROR", "error": str(e), "resolved_bindings": {}, "ambiguities": []}
        )

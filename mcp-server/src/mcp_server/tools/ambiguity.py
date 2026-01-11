"""Tools for detecting and resolving query ambiguity."""

import json
import logging
from typing import Any, Dict, List, Optional

from mcp_server.services.ambiguity.resolver import AmbiguityResolver

logger = logging.getLogger(__name__)

# Singleton resolver for the tool
_resolver: Optional[AmbiguityResolver] = None


def get_resolver() -> AmbiguityResolver:
    """Get or initialize the singleton resolver."""
    global _resolver
    if _resolver is None:
        _resolver = AmbiguityResolver()
    return _resolver


async def resolve_ambiguity(query: str, schema_context: List[Dict[str, Any]]) -> str:
    """Resolve potential ambiguities in a user query against provided schema context.

    Returns JSON string with resolution status and bindings.
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

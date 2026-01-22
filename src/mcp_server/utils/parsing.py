import json
from typing import Any


def format_result_for_tool(result: Any) -> str:
    """Format service results for standardized MCP tool output.

    Handles None, strings, and Pydantic models (like CacheLookupResult).
    Returns "MISSING" for None or empty results in a cache context,
    or a JSON string for everything else.
    """
    if result is None:
        return "MISSING"

    if isinstance(result, str):
        return result

    # Handle Pydantic models (like CacheLookupResult)
    if hasattr(result, "model_dump"):
        return json.dumps(result.model_dump(), separators=(",", ":"))

    try:
        return json.dumps(result, separators=(",", ":"))
    except (TypeError, ValueError):
        return str(result)

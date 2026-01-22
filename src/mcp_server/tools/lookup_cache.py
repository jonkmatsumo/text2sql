"""MCP tool: lookup_cache - Look up a query in the semantic registry cache."""

from mcp_server.services.cache import lookup_cache as lookup_cache_svc
from mcp_server.utils.parsing import format_result_for_tool

TOOL_NAME = "lookup_cache"


async def handler(query: str, tenant_id: int) -> str:
    """Look up a query in the semantic registry cache.

    Args:
        query: The user query to look up.
        tenant_id: Tenant identifier for cache lookup.

    Returns:
        The cached SQL result if a semantic match is found, or "MISSING".
    """
    result = await lookup_cache_svc(query, tenant_id)
    return format_result_for_tool(result)

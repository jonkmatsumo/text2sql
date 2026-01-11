"""MCP tool: update_cache - Update the semantic registry with a new query-SQL pair."""

from mcp_server.services.cache import update_cache as update_cache_svc

TOOL_NAME = "update_cache"


async def handler(query: str, sql: str, tenant_id: int) -> str:
    """Update the semantic registry with a new confirmed query-SQL pair.

    Args:
        query: The user query.
        sql: The SQL query that corresponds to the user query.
        tenant_id: Tenant identifier for the cache entry.

    Returns:
        "OK" on success.
    """
    await update_cache_svc(query, sql, tenant_id)
    return "OK"

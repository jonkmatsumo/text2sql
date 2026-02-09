"""MCP tool: lookup_cache - Look up a query in the semantic registry cache."""

from mcp_server.services.cache import lookup_cache as lookup_cache_svc

TOOL_NAME = "lookup_cache"
TOOL_DESCRIPTION = "Look up a query in the semantic registry cache."


async def handler(query: str, tenant_id: int) -> str:
    """Look up a query in the semantic registry cache.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher) and valid 'tenant_id'.

    Data Access:
        Read-only access to the semantic cache store. Scoped by tenant_id.

    Failure Modes:
        - Unauthorized: If tenant_id is missing or role is insufficient.
        - Cache Miss: Returns "MISSING" if no semantic match is found.
        - Dependency Failure: If the cache service is unavailable.

    Args:
        query: The user query to look up.
        tenant_id: Tenant identifier for cache lookup.

    Returns:
        JSON string containing the cached SQL result or "MISSING".
    """
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err
    import time

    start_time = time.monotonic()

    result = await lookup_cache_svc(query, tenant_id)

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    # If result is None, we still return "MISSING" in the result field for now
    # to maintain consistency with historical behavior.
    # Or better, we return the dict if it's there.
    inner_result = result.model_dump() if result and hasattr(result, "model_dump") else result
    if inner_result is None:
        inner_result = "MISSING"

    # Note: cache logic is dummy for now
    envelope = ToolResponseEnvelope(
        result=inner_result,
        metadata=GenericToolMetadata(provider="cache_service", execution_time_ms=execution_time_ms),
    )
    return envelope.model_dump_json(exclude_none=True)

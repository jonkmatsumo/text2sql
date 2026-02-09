"""MCP tool: update_cache - Update the semantic registry with a new query-SQL pair."""

from mcp_server.services.cache import update_cache as update_cache_svc

TOOL_NAME = "update_cache"
TOOL_DESCRIPTION = "Update the semantic registry with a new confirmed query-SQL pair."


async def handler(query: str, sql: str, tenant_id: int, schema_snapshot_id: str = None) -> str:
    """Update the semantic registry with a new confirmed query-SQL pair.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher) and valid 'tenant_id'.

    Data Access:
        Write access to the semantic cache store. Scoped by tenant_id.

    Failure Modes:
        - Unauthorized: If tenant_id is missing or role is insufficient.
        - Cache Update Failed: If the cache service fails to store the pair.

    Args:
        query: The user query.
        sql: The SQL query that corresponds to the user query.
        tenant_id: Tenant identifier for the cache entry.
        schema_snapshot_id: Optional schema snapshot identifier to verify consistency.

    Returns:
        JSON envelope with "OK" status.
    """
    import time

    from common.models.error_metadata import ErrorMetadata
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from dal.database import Database
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    start_time = time.monotonic()

    try:
        await update_cache_svc(query, sql, tenant_id, schema_snapshot_id=schema_snapshot_id)

        execution_time_ms = (time.monotonic() - start_time) * 1000
        return ToolResponseEnvelope(
            result="OK",
            metadata=GenericToolMetadata(
                provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
            ),
        ).model_dump_json(exclude_none=True)
    except Exception as e:
        return ToolResponseEnvelope(
            result={"success": False, "error": str(e)},
            error=ErrorMetadata(
                message=str(e),
                category="cache_update_failed",
                provider="cache_service",
                is_retryable=False,
            ),
        ).model_dump_json(exclude_none=True)

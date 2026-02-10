"""MCP tool for schema hydration."""

from mcp_server.services.ops.maintenance import MaintenanceService

TOOL_NAME = "hydrate_schema"
TOOL_DESCRIPTION = "Trigger schema hydration from Postgres to Graph store."


async def handler(dry_run: bool = False) -> str:
    """Trigger schema hydration from Postgres to Graph store.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read access to Postgres schema metadata. Write access to the Graph store
        (Memgraph) and RAG vector store.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Maintenance Failed: If the hydration process encounters errors during transfer.

    Args:
        dry_run: If True, skips writing to the Graph store.

    Returns:
        JSON string containing hydration logs and success status.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.errors import build_error_metadata

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
        return err

    try:
        results = []
        async for msg in MaintenanceService.hydrate_schema():
            results.append(msg)

        execution_time_ms = (time.monotonic() - start_time) * 1000

        return ToolResponseEnvelope(
            result={"success": True, "logs": results},
            metadata=GenericToolMetadata(
                provider="maintenance_service", execution_time_ms=execution_time_ms
            ),
        ).model_dump_json(exclude_none=True)
    except Exception as e:
        _ = e  # keep local exception for logging/debugging only
        error_code = "MAINTENANCE_FAILED"
        return ToolResponseEnvelope(
            result={"success": False, "error": {"code": error_code}},
            error=build_error_metadata(
                message="Schema hydration failed.",
                category="maintenance_failed",
                provider="maintenance_service",
                retryable=False,
                code=error_code,
            ),
        ).model_dump_json(exclude_none=True)

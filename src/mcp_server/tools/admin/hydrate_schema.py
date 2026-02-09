"""MCP tool for schema hydration."""

from mcp_server.services.ops.maintenance import MaintenanceService

TOOL_NAME = "hydrate_schema"


async def handler(dry_run: bool = False) -> str:
    """Trigger schema hydration from Postgres to Graph store."""
    import time

    from common.models.error_metadata import ErrorMetadata
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

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
        return ToolResponseEnvelope(
            result={"success": False, "error": str(e)},
            error=ErrorMetadata(
                message=str(e),
                category="maintenance_failed",
                provider="maintenance_service",
                is_retryable=False,
            ),
        ).model_dump_json(exclude_none=True)

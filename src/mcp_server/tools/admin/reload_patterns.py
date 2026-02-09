from mcp_server.services.canonicalization.pattern_reload_service import (
    PatternReloadService,
    ReloadResult,
)

TOOL_NAME = "reload_patterns"
TOOL_DESCRIPTION = "Reload NLP patterns from the database without restarting the application."


async def handler() -> str:
    """Reload NLP patterns from the database without restarting the application.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read-only access to the patterns table in the database and write access to the
        in-memory EntityRuler patterns.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Service Error: If the pattern reload service fails to fetch or apply patterns.

    Returns:
        JSON string containing the status and details of the reload operation.
    """
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
        return err

    # Pattern reload service tracks its own duration, but we'll wrap it
    result: ReloadResult = await PatternReloadService.reload(source="admin_tool")

    payload = {
        "success": result.success,
        "error": result.error,
        "reloaded_at": result.reloaded_at.isoformat(),
        "pattern_count": result.pattern_count,
        "reload_id": result.reload_id,
        "duration_ms": result.duration_ms,
    }

    return ToolResponseEnvelope(
        result=payload,
        metadata=GenericToolMetadata(
            provider="pattern_reload_service", execution_time_ms=result.duration_ms
        ),
    ).model_dump_json(exclude_none=True)

from mcp_server.services.canonicalization.pattern_reload_service import (
    PatternReloadService,
    ReloadResult,
)

TOOL_NAME = "reload_patterns"


async def handler() -> str:
    """Reload NLP patterns from the database without restarting the application.

    This tool triggers an atomic reload of the EntityRuler patterns.
    It returns the status of the reload operation including pattern count.
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

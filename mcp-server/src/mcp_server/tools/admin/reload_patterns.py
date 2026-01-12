from typing import Any, Dict

from mcp_server.services.canonicalization.pattern_reload_service import (
    PatternReloadService,
    ReloadResult,
)


async def handler() -> Dict[str, Any]:
    """Reload NLP patterns from the database without restarting the application.

    This tool triggers an atomic reload of the EntityRuler patterns.
    It returns the status of the reload operation including pattern count.
    """
    result: ReloadResult = await PatternReloadService.reload(source="admin_tool")
    return {
        "success": result.success,
        "error": result.error,
        "reloaded_at": result.reloaded_at.isoformat(),
        "pattern_count": result.pattern_count,
    }

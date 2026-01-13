"""Operations Service for Streamlit Admin Panel."""

import logging
from typing import AsyncGenerator

from mcp_server.services.ops.maintenance import MaintenanceService

logger = logging.getLogger(__name__)


class OpsService:
    """Bridge service for Admin Operations."""

    @staticmethod
    async def run_pattern_generation(dry_run: bool = False) -> AsyncGenerator[str, None]:
        """Run pattern generation and yield logs."""
        async for log in MaintenanceService.generate_patterns(dry_run=dry_run):
            yield log

    @staticmethod
    async def run_schema_hydration() -> AsyncGenerator[str, None]:
        """Run schema hydration and yield logs."""
        async for log in MaintenanceService.hydrate_schema():
            yield log

    @staticmethod
    async def run_cache_reindexing() -> AsyncGenerator[str, None]:
        """Run cache re-indexing and yield logs."""
        async for log in MaintenanceService.reindex_cache():
            yield log

    @staticmethod
    async def reload_patterns() -> dict:
        """Trigger backend pattern reload and return result."""
        from mcp_server.services.canonicalization.pattern_reload_service import (
            PatternReloadService,
            ReloadResult,
        )

        result: ReloadResult = await PatternReloadService.reload(source="admin_ui")
        return {
            "success": result.success,
            "message": (
                "Patterns reloaded successfully."
                if result.success
                else f"Reload failed: {result.error}"
            ),
            "reload_id": result.reload_id,
            "duration_ms": result.duration_ms,
            "pattern_count": result.pattern_count,
        }

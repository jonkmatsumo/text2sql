"""MCP tool for semantic cache re-indexing."""

from mcp_server.services.ops.maintenance import MaintenanceService

TOOL_NAME = "reindex_semantic_cache"
TOOL_DESCRIPTION = "Trigger re-indexing of the semantic cache."


async def handler(dry_run: bool = False) -> str:
    """Trigger re-indexing of the semantic cache.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read access to the semantic cache store and write access to the vector indices.

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Maintenance Failed: If the re-indexing process encounters errors.

    Args:
        dry_run: If True, skips writing to the vector store.

    Returns:
        JSON string containing re-indexing logs and success status.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.errors import build_error_metadata

    start_time = time.monotonic()

    from mcp_server.utils.auth import require_admin

    if err := require_admin(TOOL_NAME):
        return err

    try:
        results = []
        async for msg in MaintenanceService.reindex_cache():
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
                message="Semantic cache reindex failed.",
                category="maintenance_failed",
                provider="maintenance_service",
                retryable=False,
                code=error_code,
            ),
        ).model_dump_json(exclude_none=True)

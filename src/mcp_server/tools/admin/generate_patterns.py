from dal.factory import get_pattern_run_store
from mcp_server.services.ops.maintenance import MaintenanceService

TOOL_NAME = "generate_patterns"
TOOL_DESCRIPTION = "Generate EntityRuler patterns from database schema interactions."


async def handler(dry_run: bool = False) -> str:
    """Generate EntityRuler patterns from database schema interactions.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read access to schema metadata and interaction logs. Write access to the
        nlp_patterns table (unless dry_run is True).

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Generation Failed: If LLM enrichment or validation fails.
        - Database Error: If the pattern store is unavailable.

    Args:
        dry_run: If True, skips writing to the database.

    Returns:
        JSON string containing run status and metrics.
    """
    # We collect the logs but primary goal is to return the final run status
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.utils.errors import build_error_metadata

    start_time = time.monotonic()

    from mcp_server.utils.auth import require_admin

    if err := require_admin(TOOL_NAME):
        return err

    logs = []
    run_id = None

    try:
        async for log in MaintenanceService.generate_patterns(dry_run=dry_run):
            logs.append(log)
            # Try to extract Run ID from the first log message if possible
            if "Run ID:" in log and run_id is None:
                try:
                    import re

                    match = re.search(r"Run ID: ([a-f0-9\-]+)", log)
                    if match:
                        run_id = match.group(1)
                except Exception:
                    pass

        # Fetch final status from the run store
        if run_id:
            run_store = get_pattern_run_store()
            run = await run_store.get_run(run_id)
            if run:
                execution_time_ms = (time.monotonic() - start_time) * 1000
                return ToolResponseEnvelope(
                    result={
                        "success": run.status == "COMPLETED",
                        "run_id": str(run_id),
                        "status": run.status,
                        "metrics": run.metrics,
                        "error": run.error_message,
                    },
                    metadata=GenericToolMetadata(
                        provider="pattern_generator", execution_time_ms=execution_time_ms
                    ),
                ).model_dump_json(exclude_none=True)

        # Fallback if run info not found
        execution_time_ms = (time.monotonic() - start_time) * 1000
        return ToolResponseEnvelope(
            result={
                "success": any("successfully saved" in log.lower() for log in logs),
                "run_id": run_id,
                "logs": logs[-3:] if logs else [],
            },
            metadata=GenericToolMetadata(
                provider="pattern_generator", execution_time_ms=execution_time_ms
            ),
        ).model_dump_json(exclude_none=True)

    except Exception as e:
        _ = e  # keep local exception for logging/debugging only
        error_code = "PATTERN_GENERATION_FAILED"
        return ToolResponseEnvelope(
            result={
                "success": False,
                "error": {"code": error_code},
                "run_id": run_id,
            },
            error=build_error_metadata(
                message="Pattern generation failed.",
                category="generation_failed",
                provider="pattern_generator",
                retryable=False,
                code=error_code,
            ),
        ).model_dump_json(exclude_none=True)

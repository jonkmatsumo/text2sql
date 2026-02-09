from dal.factory import get_pattern_run_store
from mcp_server.services.ops.maintenance import MaintenanceService

TOOL_NAME = "generate_patterns"


async def handler(dry_run: bool = False) -> str:
    """Generate EntityRuler patterns from database schema interactions.

    This tool triggers the pattern generation pipeline, which:
    1. Introspects table and column schemas.
    2. Uses LLM enrichment for colloquial synonyms.
    3. Validates and writes patterns to the nlp_patterns table.

    Args:
        dry_run: If True, skips writing to the database.

    Returns:
        JSON compatible dictionary with run status and metrics.
    """
    # We collect the logs but primary goal is to return the final run status
    import time

    from common.models.error_metadata import ErrorMetadata
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
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
        return ToolResponseEnvelope(
            result={
                "success": False,
                "error": str(e),
                "run_id": run_id,
            },
            error=ErrorMetadata(
                message=str(e),
                category="generation_failed",
                provider="pattern_generator",
                is_retryable=False,
            ),
        ).model_dump_json(exclude_none=True)

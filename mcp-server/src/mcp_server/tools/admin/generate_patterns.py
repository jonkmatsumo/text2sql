from typing import Any, Dict

from mcp_server.services.ops.maintenance import MaintenanceService

from dal.factory import get_pattern_run_store


async def handler(dry_run: bool = False) -> Dict[str, Any]:
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
                return {
                    "success": run.status == "COMPLETED",
                    "run_id": str(run_id),
                    "status": run.status,
                    "metrics": run.metrics,
                    "error": run.error_message,
                }

        # Fallback if run info not found
        return {
            "success": any("successfully saved" in log.lower() for log in logs),
            "run_id": run_id,
            "logs": logs[-3:] if logs else [],
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "run_id": run_id,
        }

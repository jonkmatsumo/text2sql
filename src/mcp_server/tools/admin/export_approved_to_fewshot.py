"""MCP tool: export_approved_to_fewshot - Sync approved interactions to Few-Shot Registry."""

from dal.factory import get_feedback_store

TOOL_NAME = "export_approved_to_fewshot"
TOOL_DESCRIPTION = "Sync approved interactions to the Few-Shot Registry."


async def handler(limit: int = 10) -> str:
    """Sync approved interactions to the Few-Shot Registry.

    Authorization:
        Requires 'ADMIN_ROLE' for execution.

    Data Access:
        Read access to the feedback store. Write access to the Few-Shot Registry
        and the feedback store (to update publish status).

    Failure Modes:
        - Unauthorized: If the required role is missing.
        - Validation Error: If limit is out of bounds.
        - Registry Error: If the few-shot registration fails for an interaction.

    Args:
        limit: Maximum number of interactions to export (default: 10).

    Returns:
        JSON string containing a summary of exports and any errors encountered.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.services.registry.service import RegistryService
    from mcp_server.utils.errors import build_error_metadata
    from mcp_server.utils.validation import validate_limit

    if err := validate_limit(limit, TOOL_NAME):
        return err

    start_time = time.monotonic()

    from mcp_server.utils.auth import validate_role

    if err := validate_role("ADMIN_ROLE", TOOL_NAME):
        return err

    f_store = get_feedback_store()

    approved = await f_store.get_approved_interactions(limit)
    results = {"total": len(approved), "published": 0, "errors": []}
    execution_time_ms = (time.monotonic() - start_time) * 1000

    for item in approved:
        try:
            # Register in Unified Registry as 'example' + 'verified'
            await RegistryService.register_pair(
                question=item["user_nlq_text"],
                sql_query=item["corrected_sql"],
                tenant_id=item["tenant_id"],
                roles=["example"],
                status="verified",
                metadata={
                    "interaction_id": item["interaction_id"],
                    "resolution_type": item["resolution_type"],
                    "source": "user_feedback",
                },
            )

            # Mark as PUBLISHED in review queue
            await f_store.set_published_status(item["interaction_id"])
            results["published"] += 1
        except Exception as e:
            _ = e  # keep local exception for logging/debugging only
            results["errors"].append(
                {
                    "id": item["interaction_id"],
                    "error": build_error_metadata(
                        message="Failed to export interaction to few-shot registry.",
                        category="registry_error",
                        provider="registry_service",
                        retryable=False,
                        code="EXPORT_FAILED",
                    ).to_dict(),
                }
            )

    return ToolResponseEnvelope(
        result=results,
        metadata=GenericToolMetadata(
            provider="registry_service", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

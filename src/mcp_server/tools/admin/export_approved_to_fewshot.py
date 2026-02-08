"""MCP tool: export_approved_to_fewshot - Sync approved interactions to Few-Shot Registry."""

from dal.factory import get_feedback_store

TOOL_NAME = "export_approved_to_fewshot"


async def handler(limit: int = 10) -> str:
    """Sync approved interactions to the Few-Shot Registry.

    Args:
        limit: Maximum number of interactions to export (default: 10).

    Returns:
        Summary of exports with total, published count, and any errors.
    """
    import time

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from mcp_server.services.registry.service import RegistryService

    start_time = time.monotonic()

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
            results["errors"].append({"id": item["interaction_id"], "error": str(e)})

    return ToolResponseEnvelope(
        result=results,
        metadata=GenericToolMetadata(
            provider="registry_service", execution_time_ms=execution_time_ms
        ),
    ).model_dump_json(exclude_none=True)

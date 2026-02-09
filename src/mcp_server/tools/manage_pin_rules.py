"""MCP tool: manage_pin_rules - Manage pinned recommendation rules."""

from typing import List, Optional
from uuid import UUID

from dal.postgres.pinned_recommendations import PostgresPinnedRecommendationStore

TOOL_NAME = "manage_pin_rules"


async def handler(
    operation: str,
    tenant_id: int,
    rule_id: Optional[str] = None,
    match_type: Optional[str] = None,
    match_value: Optional[str] = None,
    registry_example_ids: Optional[List[str]] = None,
    priority: Optional[int] = None,
    enabled: Optional[bool] = None,
) -> str:
    """Manage pinned recommendation rules.

    Args:
        operation: One of 'list', 'upsert', 'delete'.
        tenant_id: Tenant identifier.
        rule_id: UUID of the rule (required for delete, optional for upsert).
        match_type: 'exact' or 'contains' (for upsert).
        match_value: The string to match (for upsert).
        registry_example_ids: List of UUIDs (for upsert).
        priority: Priority score (for upsert).
        enabled: Enable/disable status (for upsert).

    Returns:
        List of rules, single rule, or boolean success.
    """
    import time

    from common.models.error_metadata import ErrorMetadata
    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from dal.database import Database
    from mcp_server.utils.validation import require_tenant_id

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    start_time = time.monotonic()

    try:
        store = PostgresPinnedRecommendationStore()
        result = None

        if operation == "list":
            rules = await store.list_rules(tenant_id)
            # Convert dataclasses to dicts for JSON serialization
            result = [
                {
                    "id": str(r.id),
                    "tenant_id": r.tenant_id,
                    "match_type": r.match_type,
                    "match_value": r.match_value,
                    "registry_example_ids": r.registry_example_ids,
                    "priority": r.priority,
                    "enabled": r.enabled,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
                for r in rules
            ]

        elif operation == "upsert":
            # Check if we are updating or creating
            if rule_id:
                # Update
                updated = await store.update_rule(
                    rule_id=UUID(rule_id),
                    tenant_id=tenant_id,
                    match_type=match_type,
                    match_value=match_value,
                    registry_example_ids=registry_example_ids,
                    priority=priority,
                    enabled=enabled,
                )
                if not updated:
                    raise ValueError("Rule not found or update failed")

                r = updated
                result = {
                    "id": str(r.id),
                    "tenant_id": r.tenant_id,
                    "match_type": r.match_type,
                    "match_value": r.match_value,
                    "registry_example_ids": r.registry_example_ids,
                    "priority": r.priority,
                    "enabled": r.enabled,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }
            else:
                # Create
                if not all([match_type, match_value, registry_example_ids is not None]):
                    raise ValueError("Missing required fields for creating a rule")

                r = await store.create_rule(
                    tenant_id=tenant_id,
                    match_type=match_type,  # type: ignore
                    match_value=match_value,  # type: ignore
                    registry_example_ids=registry_example_ids,  # type: ignore
                    priority=priority or 0,
                    enabled=enabled if enabled is not None else True,
                )
                result = {
                    "id": str(r.id),
                    "tenant_id": r.tenant_id,
                    "match_type": r.match_type,
                    "match_value": r.match_value,
                    "registry_example_ids": r.registry_example_ids,
                    "priority": r.priority,
                    "enabled": r.enabled,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "updated_at": r.updated_at.isoformat() if r.updated_at else None,
                }

        elif operation == "delete":
            if not rule_id:
                raise ValueError("rule_id required for delete")

            success = await store.delete_rule(UUID(rule_id), tenant_id)
            result = {"success": success}

        else:
            raise ValueError(f"Unknown operation: {operation}")

        execution_time_ms = (time.monotonic() - start_time) * 1000
        return ToolResponseEnvelope(
            result=result,
            metadata=GenericToolMetadata(
                provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
            ),
        ).model_dump_json(exclude_none=True)

    except Exception as e:
        return ToolResponseEnvelope(
            result={"success": False, "error": str(e)},
            error=ErrorMetadata(
                message=str(e),
                category="rule_management_failed",
                provider="pinned_recommendation_store",
                is_retryable=False,
            ),
        ).model_dump_json(exclude_none=True)

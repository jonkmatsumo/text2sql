"""MCP tool: manage_pin_rules - Manage pinned recommendation rules."""

from typing import Any, List, Optional
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
) -> Any:
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
    store = PostgresPinnedRecommendationStore()

    if operation == "list":
        rules = await store.list_rules(tenant_id)
        # Convert dataclasses to dicts for JSON serialization
        return [
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
                return {"error": "Rule not found or update failed"}

            r = updated
            return {
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
                return {"error": "Missing required fields for creating a rule"}

            r = await store.create_rule(
                tenant_id=tenant_id,
                match_type=match_type,  # type: ignore
                match_value=match_value,  # type: ignore
                registry_example_ids=registry_example_ids,  # type: ignore
                priority=priority or 0,
                enabled=enabled if enabled is not None else True,
            )
            return {
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
            return {"error": "rule_id required for delete"}

        success = await store.delete_rule(UUID(rule_id), tenant_id)
        return {"success": success}

    else:
        return {"error": f"Unknown operation: {operation}"}

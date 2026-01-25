from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from dal.control_plane import ControlPlaneDatabase
from dal.database import Database


from dal.models.recommendations import PinRule



class PostgresPinnedRecommendationStore:
    """DAL for pinned_recommendations table."""

    async def list_rules(self, tenant_id: int, only_enabled: bool = False) -> List[PinRule]:
        """List rules for a tenant."""
        if ControlPlaneDatabase.is_enabled():
            conn_ctx = ControlPlaneDatabase.get_connection(tenant_id)
        else:
            conn_ctx = Database.get_connection(tenant_id)

        async with conn_ctx as conn:
            query = """
                SELECT id, tenant_id, match_type, match_value, registry_example_ids,
                       priority, enabled, created_at, updated_at
                FROM pinned_recommendations
                WHERE tenant_id = $1
            """
            args = [tenant_id]

            if only_enabled:
                query += " AND enabled = TRUE"

            query += " ORDER BY priority DESC, created_at DESC"

            rows = await conn.fetch(query, *args)
            return [self._row_to_rule(row) for row in rows]

    async def create_rule(
        self,
        tenant_id: int,
        match_type: str,
        match_value: str,
        registry_example_ids: List[str],
        priority: int = 0,
        enabled: bool = True,
    ) -> PinRule:
        """Create a new pin rule."""
        import json

        if ControlPlaneDatabase.is_enabled():
            conn_ctx = ControlPlaneDatabase.get_connection(tenant_id)
        else:
            conn_ctx = Database.get_connection(tenant_id)

        async with conn_ctx as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO pinned_recommendations
                (tenant_id, match_type, match_value, registry_example_ids, priority, enabled)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                RETURNING *
                """,
                tenant_id,
                match_type,
                match_value,
                json.dumps(registry_example_ids),
                priority,
                enabled,
            )
            return self._row_to_rule(row)

    async def update_rule(
        self,
        rule_id: UUID,
        tenant_id: int,
        match_type: Optional[str] = None,
        match_value: Optional[str] = None,
        registry_example_ids: Optional[List[str]] = None,
        priority: Optional[int] = None,
        enabled: Optional[bool] = None,
    ) -> Optional[PinRule]:
        """Update a rule. Requires tenant_id for safety."""
        import json

        if ControlPlaneDatabase.is_enabled():
            conn_ctx = ControlPlaneDatabase.get_connection(tenant_id)
        else:
            conn_ctx = Database.get_connection(tenant_id)

        async with conn_ctx as conn:
            # Build dynamic update
            updates = []
            args = []
            idx = 1

            if match_type is not None:
                updates.append(f"match_type = ${idx}")
                args.append(match_type)
                idx += 1
            if match_value is not None:
                updates.append(f"match_value = ${idx}")
                args.append(match_value)
                idx += 1
            if registry_example_ids is not None:
                updates.append(f"registry_example_ids = ${idx}::jsonb")
                args.append(json.dumps(registry_example_ids))
                idx += 1
            if priority is not None:
                updates.append(f"priority = ${idx}")
                args.append(priority)
                idx += 1
            if enabled is not None:
                updates.append(f"enabled = ${idx}")
                args.append(enabled)
                idx += 1

            if not updates:
                return None

            # Add ID and tenant_id filters
            args.append(str(rule_id))
            args.append(tenant_id)

            query = f"""
                UPDATE pinned_recommendations
                SET {', '.join(updates)}
                WHERE id = ${idx}::uuid AND tenant_id = ${idx+1}
                RETURNING *
            """

            row = await conn.fetchrow(query, *args)
            return self._row_to_rule(row) if row else None

    async def delete_rule(self, rule_id: UUID, tenant_id: int) -> bool:
        """Delete a rule."""
        if ControlPlaneDatabase.is_enabled():
            conn_ctx = ControlPlaneDatabase.get_connection(tenant_id)
        else:
            conn_ctx = Database.get_connection(tenant_id)

        async with conn_ctx as conn:
            res = await conn.execute(
                "DELETE FROM pinned_recommendations WHERE id = $1::uuid AND tenant_id = $2",
                str(rule_id),
                tenant_id,
            )
            return res != "DELETE 0"

    def _row_to_rule(self, row) -> PinRule:
        import json

        # Start of fix for unknown type error in tests
        registry_ids = row["registry_example_ids"]
        if isinstance(registry_ids, str):
            registry_ids = json.loads(registry_ids)
        # End of fix

        return PinRule(
            id=row["id"],
            tenant_id=row["tenant_id"],
            match_type=row["match_type"],
            match_value=row["match_value"],
            registry_example_ids=registry_ids,
            priority=row["priority"],
            enabled=row["enabled"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

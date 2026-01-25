"""Integration tests for the Admin Service."""

import os

import pytest

from ui.service.admin import AdminService


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_service_e2e_flow():
    """Test AdminService -> Tool -> Store -> DB flow."""
    tenant_id = 99999

    # Enable isolation to match target environment if not set
    if "DB_ISOLATION_ENABLED" not in os.environ:
        os.environ["DB_ISOLATION_ENABLED"] = "true"

    from unittest.mock import patch

    from dal.database import Database
    from mcp_server.tools.manage_pin_rules import handler as pin_handler

    async def mock_call_tool(tool_name, args):
        if tool_name == "manage_pin_rules":
            return await pin_handler(**args)
        raise ValueError(f"Unexpected tool call: {tool_name}")

    with patch("ui.service.admin.AdminService._call_tool", side_effect=mock_call_tool):
        # Initialize DAL since we are bypassing the server
        await Database.init()

        # Ensure schema table exists for test (local env might be missing it)
        pool = Database._pool
        if pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pinned_recommendations (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        tenant_id INTEGER NOT NULL,
                        match_type TEXT NOT NULL,
                        match_value TEXT NOT NULL,
                        registry_example_ids JSONB NOT NULL,
                        priority INTEGER DEFAULT 0,
                        enabled BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                    );
                """
                )

        try:
            # 1. Create
            rule = await AdminService.upsert_pin_rule(
                tenant_id=tenant_id,
                match_type="exact",
                match_value="integration_ui_test",
                registry_example_ids=["ex_ui"],
                priority=100,
                enabled=True,
            )
            assert rule.match_value == "integration_ui_test"
            assert rule.id is not None

            # 2. List
            rules = await AdminService.list_pin_rules(tenant_id)
            assert len(rules) >= 1
            found = next((r for r in rules if r.id == rule.id), None)
            assert found is not None
            assert found.match_value == "integration_ui_test"

            # 3. Delete
            success = await AdminService.delete_pin_rule(str(rule.id), tenant_id)
            assert success is True

            # Verify deletion
            rules = await AdminService.list_pin_rules(tenant_id)
            assert not any(r.id == rule.id for r in rules)

        except Exception as e:
            pytest.fail(f"Integration test failed: {e}")
        finally:
            await Database.close()

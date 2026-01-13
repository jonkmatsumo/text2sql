"""Integration tests for the Admin Service."""

import os

import pytest

from streamlit_app.service.admin import AdminService


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_service_e2e_flow():
    """Test AdminService -> Tool -> Store -> DB flow."""
    tenant_id = 99999

    # Enable isolation to match target environment if not set
    if "DB_ISOLATION_ENABLED" not in os.environ:
        os.environ["DB_ISOLATION_ENABLED"] = "true"

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

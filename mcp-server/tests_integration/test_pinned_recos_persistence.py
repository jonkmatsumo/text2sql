"""Integration tests for pinned recommendations persistence."""

import os

import pytest
from mcp_server.config.control_plane import ControlPlaneDatabase

from dal.postgres.pinned_recommendations import PostgresPinnedRecommendationStore


# This marker ensures it is excluded from default CI runs unless -m integration is used
@pytest.mark.integration
@pytest.mark.asyncio
async def test_pinned_persistence_in_control_plane():
    """Verify that we can write/read pin rules when isolation is enabled."""
    # We force enable this for the test context if not already
    original_isolation = os.environ.get("DB_ISOLATION_ENABLED")
    os.environ["DB_ISOLATION_ENABLED"] = "true"

    # Re-init ControlPlane to pick up env var if needed (though conftest usually handles init)
    # But ControlPlaneDatabase.init checks the env var.
    await ControlPlaneDatabase.init()

    if not ControlPlaneDatabase.is_enabled():
        pytest.skip("Control Plane DB could not be enabled (missing host?). Skipping.")

    store = PostgresPinnedRecommendationStore()
    tenant_id = 99999  # Test tenant

    # 1. Create
    rule = await store.create_rule(
        tenant_id=tenant_id,
        match_type="exact",
        match_value="integration_test_val",
        registry_example_ids=[],
        priority=50,
        enabled=True,
    )
    assert rule.id is not None
    assert rule.match_value == "integration_test_val"

    # 2. List
    rules = await store.list_rules(tenant_id)
    assert len(rules) >= 1
    found = next((r for r in rules if r.id == rule.id), None)
    assert found is not None

    # 3. Clean up
    await store.delete_rule(rule.id, tenant_id)

    # Restore env
    if original_isolation is None:
        del os.environ["DB_ISOLATION_ENABLED"]
    else:
        os.environ["DB_ISOLATION_ENABLED"] = original_isolation

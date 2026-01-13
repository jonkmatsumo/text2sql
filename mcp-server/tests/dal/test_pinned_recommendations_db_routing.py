from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from mcp_server.dal.postgres.pinned_recommendations import PostgresPinnedRecommendationStore


@pytest.fixture
def mock_db_context():
    """Mock the async context manager for DB connections."""
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_conn
    mock_ctx.__aexit__.return_value = None
    return mock_ctx, mock_conn


@pytest.mark.asyncio
async def test_list_rules_uses_control_plane_when_enabled(mock_db_context):
    """Verify list_rules uses ControlPlaneDatabase when isolation is enabled."""
    mock_ctx, mock_conn = mock_db_context
    mock_conn.fetch.return_value = []

    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.ControlPlaneDatabase"
    ) as mock_cp, patch("mcp_server.dal.postgres.pinned_recommendations.Database") as mock_db:

        # Setup: Isolation Enabled
        mock_cp.is_enabled.return_value = True
        mock_cp.get_connection.return_value = mock_ctx

        store = PostgresPinnedRecommendationStore()
        await store.list_rules(tenant_id=1)

        # Verify Control Plane was used
        mock_cp.get_connection.assert_called_once_with(1)
        mock_db.get_connection.assert_not_called()


@pytest.mark.asyncio
async def test_list_rules_uses_main_db_when_disabled(mock_db_context):
    """Verify list_rules uses Database (main) when isolation is disabled."""
    mock_ctx, mock_conn = mock_db_context
    mock_conn.fetch.return_value = []

    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.ControlPlaneDatabase"
    ) as mock_cp, patch("mcp_server.dal.postgres.pinned_recommendations.Database") as mock_db:

        # Setup: Isolation Disabled
        mock_cp.is_enabled.return_value = False
        mock_db.get_connection.return_value = mock_ctx

        store = PostgresPinnedRecommendationStore()
        await store.list_rules(tenant_id=1)

        # Verify Main DB was used
        mock_db.get_connection.assert_called_once_with(1)
        mock_cp.get_connection.assert_not_called()


@pytest.mark.asyncio
async def test_create_rule_routing(mock_db_context):
    """Verify create_rule follows routing logic."""
    mock_ctx, mock_conn = mock_db_context
    # Mock return row for create
    mock_conn.fetchrow.return_value = {
        "id": uuid4(),
        "tenant_id": 1,
        "match_type": "exact",
        "match_value": "foo",
        "registry_example_ids": [],
        "priority": 10,
        "enabled": True,
        "created_at": None,
        "updated_at": None,
    }

    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.ControlPlaneDatabase"
    ) as mock_cp, patch("mcp_server.dal.postgres.pinned_recommendations.Database") as mock_db:

        # Setup: Isolation Enabled
        mock_cp.is_enabled.return_value = True
        mock_cp.get_connection.return_value = mock_ctx

        store = PostgresPinnedRecommendationStore()
        await store.create_rule(1, "exact", "foo", [])

        # Verify Control Plane was used
        mock_cp.get_connection.assert_called_once_with(1)
        mock_db.get_connection.assert_not_called()


@pytest.mark.asyncio
async def test_update_rule_routing(mock_db_context):
    """Verify update_rule follows routing logic."""
    mock_ctx, mock_conn = mock_db_context
    mock_conn.fetchrow.return_value = (
        None  # Return None means row updated/found or not, doesn't matter for routing check
    )

    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.ControlPlaneDatabase"
    ) as mock_cp, patch("mcp_server.dal.postgres.pinned_recommendations.Database") as mock_db:

        # Setup: Isolation Disabled
        mock_cp.is_enabled.return_value = False
        mock_db.get_connection.return_value = mock_ctx

        store = PostgresPinnedRecommendationStore()
        await store.update_rule(uuid4(), 1, match_value="bar")

        # Verify Main DB was used
        mock_db.get_connection.assert_called_once_with(1)
        mock_cp.get_connection.assert_not_called()


@pytest.mark.asyncio
async def test_delete_rule_routing(mock_db_context):
    """Verify delete_rule follows routing logic."""
    mock_ctx, mock_conn = mock_db_context
    mock_conn.execute.return_value = "DELETE 1"

    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.ControlPlaneDatabase"
    ) as mock_cp, patch("mcp_server.dal.postgres.pinned_recommendations.Database") as mock_db:

        # Setup: Isolation Enabled
        mock_cp.is_enabled.return_value = True
        mock_cp.get_connection.return_value = mock_ctx

        store = PostgresPinnedRecommendationStore()
        await store.delete_rule(uuid4(), 1)

        # Verify Control Plane was used
        mock_cp.get_connection.assert_called_once_with(1)
        mock_db.get_connection.assert_not_called()

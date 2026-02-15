import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.validation.policy_loader import PolicyLoader


@pytest.fixture
def mock_control_plane():
    """Mock the ControlPlaneDatabase for testing."""
    with patch("agent.validation.policy_loader.ControlPlaneDatabase") as mock_cp:
        mock_cp.is_enabled.return_value = True
        mock_cp._pool = True  # Simulate initialized

        # Mock connection and fetch
        mock_conn = AsyncMock()
        mock_cp.get_connection.return_value.__aenter__.return_value = mock_conn

        # Default mock data
        mock_conn.fetch.return_value = [
            {
                "table_name": "t1",
                "tenant_column": "tenant_id",
                "policy_expression": "x=1",
            }
        ]
        yield mock_cp


@pytest.fixture
def mock_otel():
    """Mock OpenTelemetry tracing."""
    with patch("agent.validation.policy_loader.trace") as mock_trace:
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_trace.get_tracer.return_value = mock_tracer
        mock_tracer.start_as_current_span.return_value.__enter__.return_value = mock_span
        yield mock_span


@pytest.mark.asyncio
async def test_get_policies_loads_from_db(mock_control_plane, mock_otel):
    """Verify that get_policies loads from DB and logs telemetry."""
    # Reset state
    PolicyLoader._policies = {}
    PolicyLoader._last_load_time = 0

    policies = await PolicyLoader.get_policies()

    assert len(policies) == 1
    assert "t1" in policies
    assert policies["t1"].tenant_column == "tenant_id"

    # Verify tracing
    mock_otel.set_attribute.assert_any_call("policy_loader.source", "control_plane")
    mock_otel.set_attribute.assert_any_call("policy_loader.loaded_count", 1)


@pytest.mark.asyncio
async def test_get_policies_concurrency(mock_control_plane):
    """Verify that concurrent calls to get_policies share a single refresh."""
    # Reset state
    PolicyLoader._policies = {}
    PolicyLoader._last_load_time = 0

    # Slow down the fetch to allow overlap
    async def slow_fetch(*args):
        await asyncio.sleep(0.1)
        return [{"table_name": "t1", "tenant_column": "c", "policy_expression": "e"}]

    mock_conn = mock_control_plane.get_connection.return_value.__aenter__.return_value
    mock_conn.fetch.side_effect = slow_fetch

    # Launch concurrent requests
    results = await asyncio.gather(
        PolicyLoader.get_policies(),
        PolicyLoader.get_policies(),
        PolicyLoader.get_policies(),
    )

    # Verify only one fetch happened
    # The lock should ensure _refresh_policies is called once.
    # With double-check locking, it should strictly be called ONCE.
    assert mock_conn.fetch.call_count == 1

    for r in results:
        assert len(r) == 1

import asyncio
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from dal.database import Database
from ui_api_gateway.app import app


@pytest.fixture
async def async_client():
    """Fixture for async client."""
    from dal.control_plane import ControlPlaneDatabase

    await Database.init()
    # Explicitly init control plane if not already done by Database.init()
    if not ControlPlaneDatabase.is_configured():
        await ControlPlaneDatabase.init()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

    await ControlPlaneDatabase.close()
    await Database.close()


@pytest.mark.asyncio
async def test_enrich_job_flow(async_client: AsyncClient):
    """Test the async enrichment job flow."""
    # 1. Analyze
    resp = await async_client.post("/ops/ingestion/analyze", json={})
    assert resp.status_code == 200
    data = resp.json()
    run_id = data["run_id"]
    candidates = data["candidates"][:1]  # Take one

    # 2. Enrich (Async)
    # Mock job creation and status updates since we are testing the API logic
    with (
        patch("ui_api_gateway.app._create_job"),
        patch("ui_api_gateway.app._update_job_status_running"),
        patch("ui_api_gateway.app._update_job_progress"),
        patch("ui_api_gateway.app._update_job_status_completed"),
        patch("ui_api_gateway.app.generate_suggestions") as mock_gen,
    ):

        mock_gen.return_value = [{"id": "test", "label": "TEST", "pattern": "test synonym"}]

        resp = await async_client.post(
            "/ops/ingestion/enrich",
            json={"run_id": run_id, "selected_candidates": candidates},
        )

    assert resp.status_code == 200
    job_data = resp.json()
    assert "job_id" in job_data

    # 3. Verify results in run (the background task might have run already or we wait)
    # Since BackgroundTasks run in the same process, we might need a small wait
    await asyncio.sleep(0.5)

    resp = await async_client.get(f"/ops/ingestion/runs/{run_id}")
    assert resp.status_code == 200
    run_data = resp.json()
    assert "draft_patterns" in run_data["config_snapshot"]
    # If the mock worked, we should have suggestions
    assert len(run_data["config_snapshot"]["draft_patterns"]) > 0

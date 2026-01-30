import pytest
from httpx import ASGITransport, AsyncClient

from dal.database import Database
from ui_api_gateway.app import app


@pytest.fixture
async def async_client():
    """Fixture for async client."""
    try:
        from dal.control_plane import ControlPlaneDatabase

        await Database.init()
        await ControlPlaneDatabase.init()
    except Exception:
        pass
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    from dal.control_plane import ControlPlaneDatabase

    await ControlPlaneDatabase.close()
    await Database.close()


@pytest.mark.asyncio
async def test_list_and_get_runs(async_client: AsyncClient):
    """Test list and get ingestion runs."""
    # Create a run first
    resp = await async_client.post("/ops/ingestion/analyze", json={})
    assert resp.status_code == 200
    run_id = resp.json()["run_id"]

    # List runs
    resp = await async_client.get("/ops/ingestion/runs")
    assert resp.status_code == 200
    runs = resp.json()
    assert any(run["id"] == run_id for run in runs)

    # Get run
    resp = await async_client.get(f"/ops/ingestion/runs/{run_id}")
    assert resp.status_code == 200
    run = resp.json()
    assert run["id"] == run_id
    assert "ui_state" in run["config_snapshot"]
    assert run["config_snapshot"]["ui_state"]["current_step"] == "review_candidates"

import pytest
from httpx import ASGITransport, AsyncClient

from dal.database import Database
from ui_api_gateway.app import app


@pytest.fixture
async def async_client():
    """Fixture for async client."""
    import os

    if not os.getenv("CONTROL_DB_HOST"):
        os.environ["CONTROL_DB_HOST"] = "localhost"
        os.environ["CONTROL_DB_PORT"] = "5433"
        os.environ["CONTROL_DB_PASSWORD"] = "control_password"
    await Database.init()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    await Database.close()


@pytest.mark.asyncio
async def test_get_metrics(async_client: AsyncClient):
    """Test get ingestion metrics."""
    resp = await async_client.get("/ops/ingestion/metrics?window=7d")
    assert resp.status_code == 200
    metrics = resp.json()
    assert "total_runs" in metrics
    assert "runs_by_day" in metrics

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

    from dal.control_plane import ControlPlaneDatabase

    await Database.init()
    if not ControlPlaneDatabase.is_configured():
        await ControlPlaneDatabase.init()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
    await ControlPlaneDatabase.close()
    await Database.close()


@pytest.mark.asyncio
async def test_template_crud(async_client: AsyncClient):
    """Test template CRUD operations."""
    # Create
    resp = await async_client.post(
        "/ops/ingestion/templates",
        json={"name": "Test Template", "config": {"target_tables": ["film"]}},
    )
    assert resp.status_code == 200
    template = resp.json()
    assert template["name"] == "Test Template"
    template_id = template["id"]

    # List
    resp = await async_client.get("/ops/ingestion/templates")
    assert resp.status_code == 200
    templates = resp.json()
    assert any(t["id"] == template_id for t in templates)

    # Update
    resp = await async_client.put(
        f"/ops/ingestion/templates/{template_id}",
        json={"name": "Updated Template", "config": {"target_tables": ["actor"]}},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "Updated Template"

    # Delete
    resp = await async_client.delete(f"/ops/ingestion/templates/{template_id}")
    assert resp.status_code == 200

    # Verify deleted
    resp = await async_client.get("/ops/ingestion/templates")
    assert not any(t["id"] == template_id for t in resp.json())

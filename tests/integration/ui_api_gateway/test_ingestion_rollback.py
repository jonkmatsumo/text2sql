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
async def test_rollback_flow(async_client: AsyncClient):
    """Test full flow with rollback."""
    # 0. Cleanup existing test data
    async with Database.get_connection(tenant_id=1) as conn:
        await conn.execute("DELETE FROM nlp_patterns WHERE label = 'ROLLBACK_TEST'")
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM nlp_patterns WHERE label = 'ROLLBACK_TEST'"
        )
        print(f"DEBUG Step 0 Count: {count}")

    # 1. Analyze
    resp = await async_client.post("/ops/ingestion/analyze", json={})
    assert resp.status_code == 200
    run_id = resp.json()["run_id"]

    # 2. Commit some patterns
    patterns = [
        {"id": "ROLLBACK_TEST_1", "label": "ROLLBACK_TEST", "pattern": "rollback synonym 1"},
        {"id": "ROLLBACK_TEST_2", "label": "ROLLBACK_TEST", "pattern": "rollback synonym 2"},
    ]
    resp = await async_client.post(
        "/ops/ingestion/commit", json={"run_id": run_id, "approved_patterns": patterns}
    )
    assert resp.status_code == 200

    # Verify in DB
    async with Database.get_connection(tenant_id=1) as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM nlp_patterns WHERE label = 'ROLLBACK_TEST'"
        )
        assert count == 2
        actions = await conn.fetch(
            "SELECT action FROM nlp_pattern_run_items WHERE run_id = $1", run_id
        )
        print(f"DEBUG Actions: {[r['action'] for r in actions]}")

    # 3. List patterns for run
    resp = await async_client.get(f"/ops/ingestion/runs/{run_id}/patterns")
    assert resp.status_code == 200
    run_patterns = resp.json()
    assert len(run_patterns) == 2

    # 4. Rollback
    resp = await async_client.post(
        f"/ops/ingestion/runs/{run_id}/rollback", json={"confirm_run_id": run_id}
    )
    assert resp.status_code == 200

    # Verify in DB
    async with Database.get_connection(tenant_id=1) as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM nlp_patterns WHERE label = 'ROLLBACK_TEST' AND deleted_at IS NULL"
        )
        assert count == 0

        # Verify it still exists but is soft-deleted
        count_all = await conn.fetchval(
            "SELECT COUNT(*) FROM nlp_patterns WHERE label = 'ROLLBACK_TEST'"
        )
        assert count_all == 2

        status = await conn.fetchval("SELECT status FROM nlp_pattern_runs WHERE id = $1", run_id)
        assert status == "ROLLED_BACK"

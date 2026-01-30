from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from dal.database import Database
from ui_api_gateway.app import app


@pytest.fixture
async def async_client():
    """Create an async client for testing."""
    # Initialize Database for the test session
    try:
        await Database.init()
    except Exception as e:
        print(f"DB Init failed: {e}")
        # Pass through, might fail later

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

    await Database.close()


@pytest.mark.asyncio
async def test_ingestion_wizard_flow(async_client: AsyncClient):
    """Test the full ingestion wizard flow via API."""
    # 1. Analyze
    analyze_payload = {"target_tables": ["category", "film_rating"]}

    # We mock detect_candidates to avoid needing a fully populated DB or slow introspection
    # and to ensure deterministic output for the test.
    mock_candidates = {
        "candidates": [
            {
                "table": "film_rating",
                "column": "rating",
                "values": ["G", "PG", "PG-13"],
                "label": "RATING",
            }
        ],
        "trusted_patterns": [],
    }

    with patch("ui_api_gateway.app.detect_candidates", return_value=mock_candidates):
        resp = await async_client.post("/ops/ingestion/analyze", json=analyze_payload)

    assert resp.status_code == 200
    data = resp.json()
    assert "run_id" in data
    assert len(data["candidates"]) == 1
    run_id = data["run_id"]

    # Verify run created in DB
    async with Database.get_connection(tenant_id=1) as conn:
        row = await conn.fetchrow("SELECT status FROM nlp_pattern_runs WHERE id = $1", run_id)
        assert row["status"] == "AWAITING_REVIEW"

    # 2. Enrich
    candidate = data["candidates"][0]
    enrich_payload = {"run_id": run_id, "selected_candidates": [candidate]}

    # Mock LLM suggestions
    mock_suggestions = [
        {"id": "G", "label": "RATING", "pattern": "general audience", "accepted": True},
        {"id": "PG", "label": "RATING", "pattern": "parental guidance", "accepted": True},
    ]

    # We patch inside ui_api_gateway.app or ingestion.patterns.generator depending on import
    # app.py imports: from ingestion.patterns.generator import detect_candidates,
    # generate_suggestions
    # So we should patch ui_api_gateway.app.generate_suggestions

    with patch("ui_api_gateway.app.generate_suggestions", return_value=mock_suggestions):
        with patch("ui_api_gateway.app.EnumLikeColumnDetector"):  # Mock detector
            with patch(
                "ingestion.patterns.generator.get_openai_client",
                return_value="mock_client",
            ):
                resp = await async_client.post("/ops/ingestion/enrich", json=enrich_payload)

    assert resp.status_code == 200
    enrich_data = resp.json()
    assert len(enrich_data["suggestions"]) == 2

    # Verify snapshot update
    async with Database.get_connection(tenant_id=1) as conn:
        row = await conn.fetchrow(
            "SELECT config_snapshot FROM nlp_pattern_runs WHERE id = $1", run_id
        )
        snapshot = row["config_snapshot"]
        # snapshot is a string in asyncpg if not decoded, but app.py handles it.
        # Here just verify it's not empty
        assert snapshot is not None

    # 3. Commit
    # Mock suggestions to commit (user might have edited them)
    suggestions_to_commit = [
        {"id": "G", "label": "RATING", "pattern": "general audience", "accepted": True},
        {
            "id": "PG",
            "label": "RATING",
            "pattern": "parental guidance",
            "accepted": True,
            "is_new": True,
        },
    ]

    commit_payload = {"run_id": run_id, "approved_patterns": suggestions_to_commit}

    # Mock background tasks? FastAPI BackgroundTasks usually just run.
    # But we mock _create_job and _run_ops_job to avoid actual job execution

    with patch("ui_api_gateway.app._create_job"):
        resp = await async_client.post("/ops/ingestion/commit", json=commit_payload)

    assert resp.status_code == 200
    commit_data = resp.json()
    assert commit_data["inserted_count"] == 2
    assert "hydration_job_id" in commit_data

    # Verify DB insertion
    async with Database.get_connection(tenant_id=1) as conn:
        rows = await conn.fetch("SELECT * FROM nlp_patterns WHERE label = 'RATING'")
        patterns = [r["pattern"] for r in rows]
        assert "general audience" in patterns
        assert "parental guidance" in patterns

        # Verify run status
        row = await conn.fetchrow("SELECT status FROM nlp_pattern_runs WHERE id = $1", run_id)
        assert row["status"] == "COMPLETED"

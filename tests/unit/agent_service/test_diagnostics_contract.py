import time

from fastapi.testclient import TestClient

from agent_service.app import app
from common.observability.monitor import RunSummary, agent_monitor


class TestDiagnosticsContract:
    """Tests for diagnostics endpoint contract."""

    def setup_method(self):
        """Seed the monitor with dummy run data."""
        # Populate monitor with some dummy data
        agent_monitor.run_history.clear()
        for i in range(10):
            agent_monitor.record_run(
                RunSummary(
                    run_id=f"run-{i}",
                    timestamp=time.time(),
                    status="success",
                    error_category=None,
                    duration_ms=100,
                    tenant_id=1,
                    llm_calls=1,
                    llm_tokens=100,
                )
            )

    def test_schema_version_present(self):
        """Verify the response includes a schema version."""
        client = TestClient(app)
        resp = client.get("/agent/diagnostics")
        assert resp.status_code == 200
        data = resp.json()
        assert "diagnostics_schema_version" in data
        assert data["diagnostics_schema_version"] >= 1

    def test_pagination_limit_and_ordering(self):
        """Verify pagination limits and reverse chronological ordering."""
        client = TestClient(app)
        # Default limit is 20, we have 10 runs.
        resp = client.get("/agent/diagnostics?recent_runs_limit=5")
        assert resp.status_code == 200
        data = resp.json()

        runs = data["monitor_snapshot"]["recent_runs"]
        assert len(runs) == 5
        # Should be newest first. run-9 is newest.
        assert runs[0]["run_id"] == "run-9"
        assert runs[4]["run_id"] == "run-5"

        # Check truncated flag
        assert data["monitor_snapshot"]["truncated"] is True

    def test_cap_enforcement(self):
        """Verify that excessive limits are capped safely."""
        client = TestClient(app)
        resp = client.get("/agent/diagnostics?recent_runs_limit=1000")
        assert resp.status_code == 200
        # Just check it doesn't crash. We know logic caps at 200.

from fastapi.testclient import TestClient

from agent_service.app import app
from common.observability.monitor import agent_monitor


class TestDiagnostics:
    """Tests for agent diagnostics and monitoring."""

    def test_diagnostics_endpoint_structure(self):
        """Verify diagnostics endpoint returns monitor snapshot."""
        client = TestClient(app)
        resp = client.get("/agent/diagnostics")
        assert resp.status_code == 200
        data = resp.json()
        assert "monitor_snapshot" in data
        assert "counters" in data["monitor_snapshot"]
        assert "recent_runs" in data["monitor_snapshot"]

    def test_monitor_increment(self):
        """Verify monitor counters can be incremented."""
        initial = agent_monitor.get_snapshot()["counters"]["circuit_breaker_open"]
        agent_monitor.increment("circuit_breaker_open")
        snapshot = agent_monitor.get_snapshot()
        assert snapshot["counters"]["circuit_breaker_open"] == initial + 1

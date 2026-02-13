import json

from fastapi.testclient import TestClient

from agent.audit import AuditEventType, emit_audit_event, reset_audit_event_buffer
from agent.state.run_summary_store import get_run_summary_store, reset_run_summary_store
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

    def test_diagnostics_recent_run_list_is_safe_subset(self, monkeypatch):
        """Recent run listing should expose a constrained, non-sensitive subset."""
        monkeypatch.setenv("OPS_RUN_SUMMARY_BUFFER_SIZE", "5")
        reset_run_summary_store()
        store = get_run_summary_store()
        store.record(
            run_id="run-safe-list",
            summary={
                "tenant_id": 11,
                "terminated_reason": "timeout",
                "replay_mode": True,
                "current_sql": "SELECT * FROM secrets",
                "query_result": [{"ssn": "123-45-6789"}],
            },
        )

        client = TestClient(app)
        resp = client.get("/agent/diagnostics?recent_runs_limit=10")
        assert resp.status_code == 200
        data = resp.json()
        recent = data["run_summary_store"]["recent_runs"]
        assert len(recent) >= 1
        assert recent[0]["run_id"] == "run-safe-list"
        assert recent[0]["terminated_reason"] == "timeout"
        assert recent[0]["tenant_id"] == 11
        assert recent[0]["replay_mode"] is True

        payload_text = json.dumps(data["run_summary_store"])
        assert "current_sql" not in payload_text
        assert "query_result" not in payload_text
        assert "ssn" not in payload_text

        reset_run_summary_store()

    def test_diagnostics_fetch_run_summary_by_run_id(self, monkeypatch):
        """Fetching by run_id should return the stored safe run decision summary."""
        monkeypatch.setenv("OPS_RUN_SUMMARY_BUFFER_SIZE", "5")
        reset_run_summary_store()
        store = get_run_summary_store()
        store.record(
            run_id="run-fetch",
            summary={
                "tenant_id": 22,
                "terminated_reason": "success",
                "replay_mode": False,
                "llm_calls": 2,
            },
        )

        client = TestClient(app)
        resp = client.get("/agent/diagnostics?run_id=run-fetch")
        assert resp.status_code == 200
        data = resp.json()
        selected = data["run_summary_store"]["selected_run"]
        assert selected["run_id"] == "run-fetch"
        assert selected["summary"]["tenant_id"] == 22
        assert selected["summary"]["llm_calls"] == 2

        payload_text = json.dumps(selected)
        assert "current_sql" not in payload_text
        assert "query_result" not in payload_text

        reset_run_summary_store()

    def test_run_summary_buffer_evicts_oldest_entries(self, monkeypatch):
        """Run summary buffer should evict the oldest entries when full."""
        monkeypatch.setenv("OPS_RUN_SUMMARY_BUFFER_SIZE", "2")
        reset_run_summary_store()
        store = get_run_summary_store()
        store.record(run_id="run-1", summary={"tenant_id": 1, "terminated_reason": "error"})
        store.record(run_id="run-2", summary={"tenant_id": 1, "terminated_reason": "error"})
        store.record(run_id="run-3", summary={"tenant_id": 1, "terminated_reason": "success"})

        client = TestClient(app)
        resp = client.get("/agent/diagnostics?recent_runs_limit=10")
        assert resp.status_code == 200
        data = resp.json()
        run_ids = [entry["run_id"] for entry in data["run_summary_store"]["recent_runs"]]
        assert run_ids == ["run-3", "run-2"]
        assert "run-1" not in run_ids

        resp_old = client.get("/agent/diagnostics?run_id=run-1")
        assert resp_old.status_code == 200
        old_selected = resp_old.json()["run_summary_store"]["selected_run"]
        assert old_selected is None

        reset_run_summary_store()

    def test_audit_diagnostics_endpoint_returns_safe_bounded_events(self, monkeypatch):
        """Audit diagnostics should expose bounded safe events without SQL payloads."""
        monkeypatch.setenv("OPS_AUDIT_EVENT_BUFFER_SIZE", "2")
        reset_audit_event_buffer()
        emit_audit_event(
            AuditEventType.POLICY_REJECTION,
            tenant_id=5,
            run_id="run-audit-safe",
            metadata={"sql": "SELECT * FROM secrets", "reason": "blocked"},
        )
        emit_audit_event(
            AuditEventType.REPLAY_MODE_ACTIVATED,
            tenant_id=5,
            run_id="run-audit-replay",
            metadata={"mode": "replay"},
        )

        client = TestClient(app)
        resp = client.get("/diagnostics/audit?limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert "recent_events" in data
        assert len(data["recent_events"]) == 2
        assert data["recent_events"][0]["run_id"] == "run-audit-replay"
        assert data["recent_events"][1]["run_id"] == "run-audit-safe"
        assert data["recent_events"][0]["source"] == "agent"
        assert "sql" not in json.dumps(data)

        filtered = client.get("/diagnostics/audit?limit=10&run_id=run-audit-safe")
        assert filtered.status_code == 200
        filtered_events = filtered.json()["recent_events"]
        assert len(filtered_events) == 1
        assert filtered_events[0]["run_id"] == "run-audit-safe"

        agent_diag = client.get("/agent/diagnostics?audit_limit=10&audit_run_id=run-audit-replay")
        assert agent_diag.status_code == 200
        audit_events = agent_diag.json()["audit_events"]
        assert len(audit_events) == 1
        assert audit_events[0]["run_id"] == "run-audit-replay"

        reset_audit_event_buffer()

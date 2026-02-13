"""Tests for structured audit event emission and retention."""

from __future__ import annotations

from agent.audit.audit_events import (
    AuditEventType,
    emit_audit_event,
    get_audit_event_buffer,
    reset_audit_event_buffer,
)


def test_audit_metadata_blocks_sql_and_row_payloads(monkeypatch):
    """Audit metadata should never retain SQL text or row payload content."""
    monkeypatch.setenv("AGENT_AUDIT_BUFFER_SIZE", "10")
    reset_audit_event_buffer()

    event = emit_audit_event(
        AuditEventType.POLICY_REJECTION,
        tenant_id=7,
        run_id="run-audit-1",
        metadata={
            "sql": "SELECT * FROM secrets",
            "rows": [{"ssn": "123-45-6789"}],
            "safe_reason": "policy_blocked",
            "note": "DELETE FROM customer",
            "nested": {"should": "drop"},
        },
    )

    assert "sql" not in event.metadata
    assert "rows" not in event.metadata
    assert "nested" not in event.metadata
    assert event.metadata["safe_reason"] == "policy_blocked"
    assert event.metadata["note"] == "<redacted_sql>"


def test_audit_buffer_is_bounded_fifo(monkeypatch):
    """Audit buffer should retain only the newest N events in FIFO order."""
    monkeypatch.setenv("AGENT_AUDIT_BUFFER_SIZE", "2")
    reset_audit_event_buffer()

    emit_audit_event(AuditEventType.POLICY_REJECTION, run_id="run-1")
    emit_audit_event(AuditEventType.REPLAY_MODE_ACTIVATED, run_id="run-2")
    emit_audit_event(AuditEventType.KILL_SWITCH_OVERRIDE, run_id="run-3")

    recent = get_audit_event_buffer().list_recent(limit=10)
    assert [item["run_id"] for item in recent] == ["run-3", "run-2"]

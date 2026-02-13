"""Tests for structured audit event emission and retention."""

from __future__ import annotations

import json

from agent.audit.audit_events import (
    AuditEventSource,
    AuditEventType,
    emit_audit_event,
    get_audit_event_buffer,
    reset_audit_event_buffer,
)


def test_audit_metadata_blocks_sql_and_row_payloads(monkeypatch):
    """Audit metadata should never retain SQL text or row payload content."""
    monkeypatch.setenv("OPS_AUDIT_EVENT_BUFFER_SIZE", "10")
    reset_audit_event_buffer()

    event = emit_audit_event(
        AuditEventType.POLICY_REJECTION,
        source=AuditEventSource.AGENT,
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
    assert event.source == AuditEventSource.AGENT.value
    assert event.metadata["safe_reason"] == "policy_blocked"
    assert event.metadata["note"] == "<redacted_sql>"


def test_audit_buffer_is_bounded_fifo(monkeypatch):
    """Audit buffer should retain only the newest N events in FIFO order."""
    monkeypatch.setenv("OPS_AUDIT_EVENT_BUFFER_SIZE", "2")
    reset_audit_event_buffer()

    emit_audit_event(AuditEventType.POLICY_REJECTION, run_id="run-1")
    emit_audit_event(AuditEventType.REPLAY_MODE_ACTIVATED, run_id="run-2")
    emit_audit_event(AuditEventType.KILL_SWITCH_OVERRIDE, run_id="run-3")

    recent = get_audit_event_buffer().list_recent(limit=10)
    assert [item["run_id"] for item in recent] == ["run-3", "run-2"]

    run_filtered = get_audit_event_buffer().list_recent(limit=10, run_id="run-2")
    assert [item["run_id"] for item in run_filtered] == ["run-2"]


def test_audit_metadata_is_bounded_after_json_encoding(monkeypatch):
    """Audit metadata JSON should be bounded and marked when truncated."""
    monkeypatch.setenv("OPS_AUDIT_EVENT_METADATA_MAX_BYTES", "2048")
    reset_audit_event_buffer()
    metadata = {f"key_{idx}": "x" * 512 for idx in range(32)}

    event = emit_audit_event(AuditEventType.POLICY_REJECTION, metadata=metadata)

    assert event.metadata.get("metadata_truncated") is True
    encoded = json.dumps(event.metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert len(encoded) <= 2048

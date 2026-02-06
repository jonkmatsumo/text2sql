"""Tests for replay bundle capture and replay helpers."""

import pytest

from agent.replay_bundle import (
    ReplayBundle,
    build_replay_bundle,
    replay_response_from_bundle,
    serialize_replay_bundle,
    validate_replay_bundle,
)


def test_build_replay_bundle_redacts_sensitive_prompts():
    """Replay bundle should redact sensitive prompt material."""
    state = {
        "current_sql": "select 1",
        "query_result": [{"id": 1}],
        "messages": [type("Msg", (), {"content": "Bearer abc123"})()],
        "schema_snapshot_id": "snap-1",
        "result_completeness": {"rows_returned": 1},
    }
    bundle = build_replay_bundle(
        question="postgresql://user:pass@localhost/db",
        state=state,
        request_payload={"tenant_id": 1, "page_token": None, "page_size": None},
    )

    assert bundle.schema_context["schema_snapshot_id"] == "snap-1"
    assert "<password>" in bundle.prompts["user"]
    assert "Bearer <redacted>" in bundle.prompts["assistant"]


def test_serialize_replay_bundle_is_deterministic():
    """Serialization should produce stable sorted JSON output."""
    bundle = ReplayBundle(
        version="1.0",
        captured_at="2026-01-01T00:00:00+00:00",
        model={"provider": "openai", "model_id": "gpt-4o"},
        seed=3,
        prompts={"user": "hi", "assistant": "ok"},
        schema_context={"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        flags={"AGENT_REPLAY_MODE": "record"},
        tool_io=[],
        outcome={"sql": "select 1", "result": [], "response": "ok"},
    )

    one = serialize_replay_bundle(bundle)
    two = serialize_replay_bundle(bundle)
    assert one == two
    assert one.startswith("{")


def test_validate_replay_bundle_schema():
    """Replay payload should validate against schema model."""
    payload = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "seed": 9,
        "prompts": {"user": "q"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {},
        "tool_io": [],
        "outcome": {"sql": "select 1", "result": []},
    }

    bundle = validate_replay_bundle(payload)
    assert bundle.version == "1.0"


def test_validate_replay_bundle_rejects_invalid_payload():
    """Missing required fields should raise validation errors."""
    with pytest.raises(Exception):
        validate_replay_bundle({"version": "1.0"})


def test_replay_response_from_bundle_uses_captured_outcome():
    """Replay helper should map captured outcome into runtime state shape."""
    bundle = ReplayBundle(
        version="1.0",
        captured_at="2026-01-01T00:00:00+00:00",
        model={"provider": "openai", "model_id": "gpt-4o"},
        seed=1,
        prompts={"user": "hello", "assistant": "captured"},
        schema_context={"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        flags={},
        tool_io=[],
        outcome={
            "sql": "select 1",
            "result": [{"id": 1}],
            "response": "captured",
            "error": None,
            "error_category": None,
            "retry_summary": {"attempts": []},
            "result_completeness": {"rows_returned": 1},
        },
    )

    replay_state = replay_response_from_bundle(bundle)
    assert replay_state["current_sql"] == "select 1"
    assert replay_state["query_result"] == [{"id": 1}]
    assert replay_state["messages"][-1].content == "captured"

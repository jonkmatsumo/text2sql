"""Tests for replay bundle capture and replay helpers."""

import pytest

from agent.replay_bundle import (
    REPLAY_BUNDLE_VERSION,
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
        "policy_snapshot": {"snapshot_id": "policy-1"},
        "run_decision_summary": {"decision_summary_hash": "hash-1"},
        "result_completeness": {"rows_returned": 1},
    }
    bundle = build_replay_bundle(
        question="postgresql://user:pass@localhost/db",
        state=state,
        request_payload={"tenant_id": 1, "page_token": None, "page_size": None},
    )

    assert bundle.schema_context["schema_snapshot_id"] == "snap-1"
    assert bundle.integrity.policy_snapshot_id == "policy-1"
    assert bundle.integrity.decision_summary_hash == "hash-1"
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


def test_validate_replay_bundle_rejects_invalid_payload():
    """Missing required fields should raise validation errors."""
    with pytest.raises(ValueError, match="Replay bundle is missing required 'version' field"):
        validate_replay_bundle({})

    with pytest.raises(ValueError, match="Incompatible replay bundle version"):
        validate_replay_bundle({"version": "0.1"})


def test_validate_replay_bundle_checks_required_fields():
    """Ensure all required top-level fields are present."""
    payload = {
        "version": REPLAY_BUNDLE_VERSION,
        "captured_at": "2026-01-01T00:00:00Z",
        # missing model, prompts, etc.
    }
    with pytest.raises(ValueError, match=r"Replay bundle is corrupted \(missing fields\)"):
        validate_replay_bundle(payload)


def test_validate_replay_bundle_schema_full():
    """Full schema validation via Pydantic."""
    payload = {
        "version": REPLAY_BUNDLE_VERSION,
        "captured_at": "2026-01-01T00:00:00Z",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "prompts": {"user": "hi", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {},
        "tool_io": [],
        "outcome": {"sql": "select 1", "result": []},
    }
    bundle = validate_replay_bundle(payload)
    assert bundle.version == REPLAY_BUNDLE_VERSION


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
    assert replay_state["execution.mode"] == "replay"


def test_build_replay_bundle_captures_raw_tool_io():
    """Replay bundle should prefer raw tool output if available in state."""
    state = {
        "current_sql": "select 1",
        "last_tool_output": {
            "rows": [{"one": 1}],
            "metadata": {"rows_returned": 1},
            "response_shape": "enveloped",
        },
        "messages": [type("Msg", (), {"content": "ok"})()],
        "schema_snapshot_id": "snap-1",
    }
    bundle = build_replay_bundle(
        question="q",
        state=state,
        request_payload={"tenant_id": 1},
    )

    assert len(bundle.tool_io) == 1
    assert bundle.tool_io[0].name == "execute_sql_query"
    assert bundle.tool_io[0].output["rows"] == [{"one": 1}]
    assert bundle.tool_io[0].output["response_shape"] == "enveloped"
    assert bundle.tool_io[0].output["metadata"]["rows_returned"] == 1

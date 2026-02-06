from fastapi.testclient import TestClient

from agent_service import app as agent_app


def test_run_agent_success(monkeypatch):
    """Return a normalized success payload from the agent service."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
    ):
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
            "from_cache": False,
            "interaction_id": "interaction-1",
            "viz_spec": {"chartType": "bar"},
            "viz_reason": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setattr(agent_app.telemetry, "get_current_trace_id", lambda: "a" * 32)

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "hi", "tenant_id": 1, "thread_id": "thread-1"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sql"] == "select 1"
    assert body["result"] == [{"one": 1}]
    assert body["response"] == "ok"
    assert body["error"] is None
    assert body["from_cache"] is False
    assert body["interaction_id"] == "interaction-1"
    assert body["trace_id"] == "a" * 32
    assert body["viz_spec"] == {"chartType": "bar"}


def test_run_agent_error(monkeypatch):
    """Return an error payload when the agent raises."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
    ):
        raise RuntimeError("boom")

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "boom"
    assert body["trace_id"] is None


def test_entrypoint_sets_deadline_ts(monkeypatch):
    """Ensure deadline_ts and timeout_seconds are passed through."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
    ):
        captured["timeout_seconds"] = timeout_seconds
        captured["deadline_ts"] = deadline_ts
        captured["interactive_session"] = interactive_session
        return {"messages": [], "error": None}

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    assert captured["timeout_seconds"] == 30.0
    assert captured["deadline_ts"] is not None
    assert captured["interactive_session"] is True


def test_run_agent_record_mode_returns_replay_bundle(monkeypatch):
    """Record mode should attach deterministic replay artifacts."""

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        timeout_seconds=None,
        deadline_ts=None,
        page_token=None,
        page_size=None,
        interactive_session=False,
    ):
        _ = question, tenant_id, thread_id, timeout_seconds, deadline_ts, page_token, page_size
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
            "schema_snapshot_id": "snap-1",
            "result_completeness": {"rows_returned": 1},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setenv("AGENT_REPLAY_MODE", "record")

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "postgresql://u:p@localhost/db", "tenant_id": 1},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["replay_bundle"] is not None
    assert body["replay_bundle_json"] is not None
    assert body["replay_metadata"]["mode"] == "record"
    assert "<password>" in body["replay_bundle"]["prompts"]["user"]


def test_run_agent_replay_mode_uses_captured_bundle_without_external_calls(monkeypatch):
    """Replay mode should return captured outcome without live execution by default."""

    async def should_not_run(*_args, **_kwargs):
        raise AssertionError("run_agent_with_tracing should not be called in captured replay mode")

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", should_not_run)
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    replay_bundle = {
        "version": "1.0",
        "captured_at": "2026-01-01T00:00:00+00:00",
        "model": {"provider": "openai", "model_id": "gpt-4o"},
        "seed": 7,
        "prompts": {"user": "show me revenue", "assistant": "ok"},
        "schema_context": {"schema_snapshot_id": "snap-1", "fingerprint": "snap-1"},
        "flags": {"AGENT_REPLAY_MODE": "replay"},
        "tool_io": [],
        "outcome": {
            "sql": "select 1",
            "result": [{"one": 1}],
            "response": "captured response",
            "error": None,
            "error_category": None,
            "retry_summary": None,
            "result_completeness": {"rows_returned": 1},
        },
    }

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={
            "question": "ignored in captured replay",
            "tenant_id": 1,
            "replay_bundle": replay_bundle,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["sql"] == "select 1"
    assert body["result"] == [{"one": 1}]
    assert body["response"] == "captured response"
    assert body["replay_metadata"]["mode"] == "replay"
    assert body["replay_metadata"]["execution"] == "captured_only"


def test_run_agent_replay_mode_validates_bundle(monkeypatch):
    """Replay mode should return validation error for malformed bundles."""
    monkeypatch.setenv("AGENT_REPLAY_MODE", "replay")

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/run",
        json={"question": "q", "tenant_id": 1, "replay_bundle": {"version": "1.0"}},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] is not None
    assert "Invalid replay bundle" in body["error"]

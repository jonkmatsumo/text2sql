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

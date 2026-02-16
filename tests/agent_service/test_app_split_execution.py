import pytest
from fastapi.testclient import TestClient

from agent_service import app as agent_app
from common.tenancy.limits import reset_agent_run_tenant_limiter


@pytest.fixture(autouse=True)
def _reset_agent_run_limiter():
    """Reset limiter singleton to avoid cross-test token/counter leakage."""
    reset_agent_run_tenant_limiter()
    yield
    reset_agent_run_tenant_limiter()


def test_generate_sql_success(monkeypatch):
    """Generate SQL endpoint should call graph with generate_only=True."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        generate_only=False,
        **kwargs,
    ):
        captured["generate_only"] = generate_only
        captured["question"] = question
        return {
            "current_sql": "SELECT 1",
            "messages": [],
            "error": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setattr(agent_app.telemetry, "get_current_trace_id", lambda: "a" * 32)

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/generate_sql",
        json={"question": "show users", "tenant_id": 1, "thread_id": "thread-1"},
    )

    assert resp.status_code == 200
    assert captured["generate_only"] is True
    assert captured["question"] == "show users"
    body = resp.json()
    assert body["sql"] == "SELECT 1"


def test_execute_sql_success(monkeypatch):
    """Execute SQL endpoint should call graph with current_sql and from_cache=True."""
    captured = {}

    async def fake_run_agent_with_tracing(
        question,
        tenant_id,
        thread_id,
        current_sql=None,
        from_cache=False,
        **kwargs,
    ):
        captured["current_sql"] = current_sql
        captured["from_cache"] = from_cache
        captured["question"] = question
        return {
            "current_sql": current_sql,
            "query_result": [{"id": 1}],
            "messages": [],
            "error": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)
    monkeypatch.setattr(agent_app.telemetry, "get_current_trace_id", lambda: "a" * 32)

    client = TestClient(agent_app.app)
    resp = client.post(
        "/agent/execute_sql",
        json={
            "question": "show users",
            "sql": "SELECT * FROM users",
            "tenant_id": 1,
            "thread_id": "thread-1",
            "replay_mode": False,
        },
    )

    assert resp.status_code == 200
    assert captured["current_sql"] == "SELECT * FROM users"
    assert captured["from_cache"] is True
    assert captured["question"] == "show users"
    body = resp.json()
    assert body["sql"] == "SELECT * FROM users"
    assert body["result"] == [{"id": 1}]

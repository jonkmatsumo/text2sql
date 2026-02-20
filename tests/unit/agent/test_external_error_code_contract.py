"""External agent response contract tests for `error_code`."""

import pytest
from fastapi.testclient import TestClient

from agent_service import app as agent_app
from common.errors.error_codes import ErrorCode
from common.tenancy.limits import reset_agent_run_tenant_limiter


@pytest.fixture(autouse=True)
def _reset_agent_run_limiter():
    """Reset limiter singleton to avoid cross-test leakage."""
    reset_agent_run_tenant_limiter()
    yield
    reset_agent_run_tenant_limiter()


def test_error_code_present_on_error(monkeypatch):
    """Error responses should include a stable canonical error_code."""

    async def fake_run_agent_with_tracing(*args, **kwargs):
        del args, kwargs
        return {
            "messages": [type("Msg", (), {"content": "failed"})()],
            "error": "Execution timed out.",
            "error_category": "timeout",
            "error_metadata": {
                "category": "timeout",
                "error_code": ErrorCode.DB_TIMEOUT.value,
            },
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] == "Database connection timed out."
    assert body["error_code"] == ErrorCode.DB_TIMEOUT.value


def test_error_code_absent_on_success(monkeypatch):
    """Successful responses should not surface an error_code value."""

    async def fake_run_agent_with_tracing(*args, **kwargs):
        del args, kwargs
        return {
            "current_sql": "select 1",
            "query_result": [{"one": 1}],
            "messages": [type("Msg", (), {"content": "ok"})()],
            "error": None,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body.get("error_code") is None


def test_error_code_derived_from_category_when_metadata_code_is_not_canonical(monkeypatch):
    """Canonical error_code should be derived from category when needed."""

    async def fake_run_agent_with_tracing(*args, **kwargs):
        del args, kwargs
        return {
            "messages": [type("Msg", (), {"content": "failed"})()],
            "error": "Validation failed.",
            "error_category": "invalid_request",
            "error_metadata": {"category": "invalid_request", "code": "DYNAMIC_DRIVER_CODE"},
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert body["error_code"] == ErrorCode.VALIDATION_ERROR.value

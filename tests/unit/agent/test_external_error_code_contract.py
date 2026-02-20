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


@pytest.mark.parametrize(
    ("error_category", "error_metadata", "expected_error_code"),
    [
        ("budget_exceeded", None, ErrorCode.DB_TIMEOUT.value),
        (
            "invalid_request",
            {"error_code": ErrorCode.AMBIGUITY_UNRESOLVED.value},
            ErrorCode.AMBIGUITY_UNRESOLVED.value,
        ),
        (
            "TENANT_ENFORCEMENT_UNSUPPORTED",
            None,
            ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value,
        ),
        ("mutation_blocked", None, ErrorCode.READONLY_VIOLATION.value),
        ("unauthorized", None, ErrorCode.SQL_POLICY_VIOLATION.value),
        ("timeout", None, ErrorCode.DB_TIMEOUT.value),
        ("connectivity", None, ErrorCode.DB_CONNECTION_ERROR.value),
        ("syntax", None, ErrorCode.DB_SYNTAX_ERROR.value),
    ],
)
def test_error_code_present_for_all_known_error_paths(
    monkeypatch,
    error_category,
    error_metadata,
    expected_error_code,
):
    """Any external error response should include a canonical error_code."""

    async def fake_run_agent_with_tracing(*args, **kwargs):
        del args, kwargs
        return {
            "messages": [type("Msg", (), {"content": "failed"})()],
            "error": "raw provider error details here",
            "error_category": error_category,
            "error_metadata": error_metadata,
        }

    monkeypatch.setattr(agent_app, "run_agent_with_tracing", fake_run_agent_with_tracing)

    client = TestClient(agent_app.app)
    resp = client.post("/agent/run", json={"question": "hi", "tenant_id": 1})

    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body.get("error"), str) and body["error"]
    assert body.get("error_code") == expected_error_code

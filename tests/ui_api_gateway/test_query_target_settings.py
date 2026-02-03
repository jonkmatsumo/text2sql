from dataclasses import replace
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from dal.query_target_config import (
    QueryTargetConfigHistoryRecord,
    QueryTargetConfigRecord,
    QueryTargetConfigStatus,
)
from dal.query_target_test import QueryTargetTestResult
from ui_api_gateway.app import app


def _record(status: QueryTargetConfigStatus) -> QueryTargetConfigRecord:
    return QueryTargetConfigRecord(
        id=uuid4(),
        provider="postgres",
        metadata={"host": "db", "db_name": "app", "user": "ro"},
        auth={},
        guardrails={},
        status=status,
    )


def test_get_query_target_settings(monkeypatch):
    """Return active and pending configs via settings endpoint."""
    client = TestClient(app)
    active = _record(QueryTargetConfigStatus.ACTIVE)
    pending = replace(
        _record(QueryTargetConfigStatus.PENDING),
        last_error_code="unsupported_provider",
    )

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _get_active():
        return active

    async def _get_pending():
        return pending

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.get_active", _get_active)
    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.get_pending", _get_pending)

    response = client.get("/settings/query-target")
    assert response.status_code == 200
    data = response.json()
    assert data["active"]["status"] == "active"
    assert data["pending"]["status"] == "pending"
    assert data["pending"]["last_error_category"] == "unsupported"


def test_upsert_query_target_settings_validation_error(monkeypatch):
    """Reject invalid payloads via settings endpoint."""
    client = TestClient(app)
    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)
    response = client.post(
        "/settings/query-target",
        json={
            "provider": "postgres",
            "metadata": {"host": "db"},
            "auth": {},
            "guardrails": {},
        },
    )
    assert response.status_code == 400


def test_upsert_query_target_settings_success(monkeypatch):
    """Persist valid settings payloads."""
    client = TestClient(app)
    record = _record(QueryTargetConfigStatus.INACTIVE)

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _upsert_config(**kwargs):
        _ = kwargs
        return record

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.upsert_config", _upsert_config)

    response = client.post(
        "/settings/query-target",
        json={
            "provider": "postgres",
            "metadata": {"host": "db", "db_name": "app", "user": "ro"},
            "auth": {},
            "guardrails": {},
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "inactive"


def test_test_query_target_settings_records_result(monkeypatch):
    """Record test results when config_id is provided."""
    client = TestClient(app)
    config_id = uuid4()
    recorded = {}

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _test_connection(*args, **kwargs):
        _ = args, kwargs
        return QueryTargetTestResult(
            ok=False, error_code="missing_secret", error_message="Missing secret"
        )

    monkeypatch.setattr("ui_api_gateway.app.test_query_target_connection", _test_connection)

    async def _record_test(config_id: UUID, status: str, error_code=None, error_message=None):
        recorded.update(
            {
                "config_id": config_id,
                "status": status,
                "error_code": error_code,
                "error_message": error_message,
            }
        )

    monkeypatch.setattr(
        "ui_api_gateway.app.QueryTargetConfigStore.record_test_result", _record_test
    )

    response = client.post(
        "/settings/query-target/test-connection",
        json={
            "provider": "postgres",
            "metadata": {"host": "db", "db_name": "app", "user": "ro"},
            "auth": {"secret_ref": "env:DB_PASS"},
            "guardrails": {},
            "config_id": str(config_id),
        },
    )
    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert response.json()["error_category"] == "auth"
    assert recorded["config_id"] == config_id
    assert recorded["status"] == "failed"


def test_activate_query_target_settings_requires_test_passed(monkeypatch):
    """Activation should fail when test-connection has not passed."""
    client = TestClient(app)
    record = QueryTargetConfigRecord(
        id=uuid4(),
        provider="postgres",
        metadata={"host": "db", "db_name": "app", "user": "ro"},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.INACTIVE,
        last_test_status="failed",
    )

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _get_by_id(_config_id):
        return record

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.get_by_id", _get_by_id)

    response = client.post(
        "/settings/query-target/activate",
        json={"config_id": str(record.id)},
    )
    assert response.status_code == 400


def test_activate_query_target_settings_marks_pending(monkeypatch):
    """Activation should mark the config as pending."""
    client = TestClient(app)
    record = QueryTargetConfigRecord(
        id=uuid4(),
        provider="postgres",
        metadata={"host": "db", "db_name": "app", "user": "ro"},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.INACTIVE,
        last_test_status="passed",
    )
    pending = QueryTargetConfigRecord(
        id=record.id,
        provider=record.provider,
        metadata=record.metadata,
        auth=record.auth,
        guardrails=record.guardrails,
        status=QueryTargetConfigStatus.PENDING,
        last_test_status=record.last_test_status,
    )

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _get_by_id(_config_id):
        return record

    async def _set_pending(_config_id):
        return None

    async def _get_pending():
        return pending

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.get_by_id", _get_by_id)
    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.set_pending", _set_pending)
    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.get_pending", _get_pending)

    response = client.post(
        "/settings/query-target/activate",
        json={"config_id": str(record.id)},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "pending"


def test_get_query_target_history_orders_by_recent(monkeypatch):
    """Return history entries ordered by most recent first."""
    client = TestClient(app)
    entry_old = QueryTargetConfigHistoryRecord(
        id=uuid4(),
        config_id=uuid4(),
        event_type="tested",
        snapshot={"provider": "postgres"},
        created_at="2025-01-01T10:00:00+00:00",
    )
    entry_new = QueryTargetConfigHistoryRecord(
        id=uuid4(),
        config_id=uuid4(),
        event_type="activated",
        snapshot={"provider": "postgres"},
        created_at="2025-01-02T10:00:00+00:00",
    )
    captured = {}

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.is_available", lambda: True)

    async def _list_history(limit: int = 50):
        captured["limit"] = limit
        return [entry_old, entry_new]

    monkeypatch.setattr("ui_api_gateway.app.QueryTargetConfigStore.list_history", _list_history)

    response = client.get("/settings/query-target/history?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert captured["limit"] == 2
    assert [item["id"] for item in data] == [str(entry_new.id), str(entry_old.id)]

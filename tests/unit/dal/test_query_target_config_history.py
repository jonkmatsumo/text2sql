from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from dal.query_target_config import QueryTargetConfigStatus
from dal.query_target_config_store import QueryTargetConfigStore


class FakeConn:
    """Capture calls for history insertion tests."""

    def __init__(self, rows):
        """Initialize with pre-seeded fetchrow results."""
        self._rows = list(rows)
        self.execute_calls = []

    async def fetchrow(self, *args, **kwargs):
        """Return the next seeded row."""
        _ = args, kwargs
        return self._rows.pop(0) if self._rows else None

    async def execute(self, *args, **kwargs):
        """Record execute calls for assertions."""
        self.execute_calls.append((args, kwargs))
        return "OK"


def _fake_row(**overrides):
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    row = {
        "id": uuid4(),
        "provider": "postgres",
        "metadata": {"host": "db"},
        "auth": {},
        "guardrails": {},
        "status": QueryTargetConfigStatus.INACTIVE.value,
        "last_tested_at": None,
        "last_test_status": None,
        "last_error_code": None,
        "last_error_message": None,
        "created_at": now,
        "updated_at": now,
        "activated_at": None,
        "deactivated_at": None,
    }
    row.update(overrides)
    return row


def _last_event_type(conn: FakeConn) -> str:
    args, _kwargs = conn.execute_calls[-1]
    return args[2]


@pytest.mark.asyncio
async def test_history_records_create(monkeypatch):
    """Create history record on insert."""
    row = _fake_row(inserted=True)
    conn = FakeConn([row])

    @asynccontextmanager
    async def _get_connection():
        yield conn

    monkeypatch.setattr(QueryTargetConfigStore, "get_connection", _get_connection)

    await QueryTargetConfigStore.upsert_config(
        provider="postgres",
        metadata={"host": "db"},
        auth={},
        guardrails={},
    )

    assert _last_event_type(conn) == "created"


@pytest.mark.asyncio
async def test_history_records_update(monkeypatch):
    """Create history record on update."""
    row = _fake_row(inserted=False)
    conn = FakeConn([row])

    @asynccontextmanager
    async def _get_connection():
        yield conn

    monkeypatch.setattr(QueryTargetConfigStore, "get_connection", _get_connection)

    await QueryTargetConfigStore.upsert_config(
        provider="postgres",
        metadata={"host": "db"},
        auth={},
        guardrails={},
    )

    assert _last_event_type(conn) == "updated"


@pytest.mark.asyncio
async def test_history_records_test(monkeypatch):
    """Create history record on test results."""
    row = _fake_row(last_test_status="failed")
    conn = FakeConn([row])

    @asynccontextmanager
    async def _get_connection():
        yield conn

    monkeypatch.setattr(QueryTargetConfigStore, "get_connection", _get_connection)

    await QueryTargetConfigStore.record_test_result(
        row["id"],
        status="failed",
        error_code="missing_secret",
        error_message="Missing secret",
    )

    assert _last_event_type(conn) == "tested"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, activated, deactivated, expected",
    [
        (QueryTargetConfigStatus.ACTIVE, True, False, "activated"),
        (QueryTargetConfigStatus.INACTIVE, False, True, "deactivated"),
        (QueryTargetConfigStatus.UNHEALTHY, False, False, "unhealthy"),
    ],
)
async def test_history_records_status_events(monkeypatch, status, activated, deactivated, expected):
    """Create history record for status transitions."""
    row = _fake_row(status=status.value)
    conn = FakeConn([row])

    @asynccontextmanager
    async def _get_connection():
        yield conn

    monkeypatch.setattr(QueryTargetConfigStore, "get_connection", _get_connection)

    await QueryTargetConfigStore.set_status(
        row["id"],
        status=status,
        activated=activated,
        deactivated=deactivated,
    )

    assert _last_event_type(conn) == expected

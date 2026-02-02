from uuid import uuid4

import pytest

from dal.query_target_config import QueryTargetConfigStatus
from dal.query_target_config_source import QueryTargetRuntimeConfig, finalize_pending_config
from dal.query_target_config_store import QueryTargetConfigStore


@pytest.mark.asyncio
async def test_finalize_pending_config_success_transitions_active(monkeypatch):
    """Pending configs should promote to active and deactivate previous."""
    pending = QueryTargetRuntimeConfig(
        id=uuid4(),
        provider="postgres",
        metadata={},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.PENDING,
    )
    active = QueryTargetRuntimeConfig(
        id=uuid4(),
        provider="postgres",
        metadata={},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.ACTIVE,
    )
    calls = []

    async def _set_status(config_id, status, **kwargs):
        calls.append((config_id, status, kwargs))

    monkeypatch.setattr(QueryTargetConfigStore, "set_status", _set_status)
    QueryTargetConfigStore._pool = object()

    try:
        await finalize_pending_config(pending, active, success=True)
    finally:
        QueryTargetConfigStore._pool = None

    assert calls[0][0] == pending.id
    assert calls[0][1] == QueryTargetConfigStatus.ACTIVE
    assert calls[0][2]["activated"] is True
    assert calls[1][0] == active.id
    assert calls[1][1] == QueryTargetConfigStatus.INACTIVE
    assert calls[1][2]["deactivated"] is True


@pytest.mark.asyncio
async def test_finalize_pending_config_failure_marks_unhealthy(monkeypatch):
    """Pending configs should be marked unhealthy on init failure."""
    pending = QueryTargetRuntimeConfig(
        id=uuid4(),
        provider="postgres",
        metadata={},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.PENDING,
    )
    calls = []

    async def _set_status(config_id, status, **kwargs):
        calls.append((config_id, status, kwargs))

    monkeypatch.setattr(QueryTargetConfigStore, "set_status", _set_status)
    QueryTargetConfigStore._pool = object()

    try:
        await finalize_pending_config(
            pending,
            None,
            success=False,
            error_message="Query-target initialization failed.",
        )
    finally:
        QueryTargetConfigStore._pool = None

    assert calls[0][0] == pending.id
    assert calls[0][1] == QueryTargetConfigStatus.UNHEALTHY
    assert calls[0][2]["error_code"] == "init_failed"
    assert calls[0][2]["error_message"] == "Query-target initialization failed."

from uuid import uuid4

import pytest

from dal.query_target_config import QueryTargetConfigRecord, QueryTargetConfigStatus
from dal.query_target_config_source import (
    QueryTargetRuntimeConfig,
    finalize_pending_config,
    load_query_target_config_selection,
)
from dal.query_target_config_store import QueryTargetConfigStore


@pytest.mark.asyncio
async def test_load_query_target_config_selection_prefers_pending(monkeypatch):
    """Selection should return both pending and active configs when available."""
    pending = QueryTargetConfigRecord(
        id=uuid4(),
        provider="postgres",
        metadata={},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.PENDING,
    )
    active = QueryTargetConfigRecord(
        id=uuid4(),
        provider="postgres",
        metadata={},
        auth={},
        guardrails={},
        status=QueryTargetConfigStatus.ACTIVE,
    )

    async def _init():
        return True

    async def _get_pending():
        return pending

    async def _get_active():
        return active

    monkeypatch.setattr(QueryTargetConfigStore, "init", _init)
    monkeypatch.setattr(QueryTargetConfigStore, "get_pending", _get_pending)
    monkeypatch.setattr(QueryTargetConfigStore, "get_active", _get_active)

    selection = await load_query_target_config_selection()
    assert selection.pending is not None
    assert selection.pending.id == pending.id
    assert selection.active is not None
    assert selection.active.id == active.id


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

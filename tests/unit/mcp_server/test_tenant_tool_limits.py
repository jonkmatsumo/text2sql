"""Tests for tenant-scoped MCP tool concurrency limits."""

from __future__ import annotations

import asyncio
import json

import pytest

from agent.audit import AuditEventType, get_audit_event_buffer, reset_audit_event_buffer
from common.tenancy.limits import reset_mcp_tool_tenant_limiter
from mcp_server.utils.tracing import trace_tool


def _ok_execute_response() -> dict:
    return {"rows": [], "metadata": {"rows_returned": 0, "is_truncated": False}}


@pytest.fixture(autouse=True)
def _reset_limiter() -> None:
    reset_mcp_tool_tenant_limiter()
    reset_audit_event_buffer()
    yield
    reset_mcp_tool_tenant_limiter()
    reset_audit_event_buffer()


@pytest.mark.asyncio
async def test_same_tenant_second_concurrent_tool_call_is_rejected(monkeypatch):
    """Second in-flight call for the same tenant should get a typed limit error."""
    monkeypatch.setenv("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", "1")
    monkeypatch.setenv("MCP_TENANT_LIMIT_RETRY_AFTER_SECONDS", "1.25")
    reset_mcp_tool_tenant_limiter()

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_handler(tenant_id: int):
        _ = tenant_id
        started.set()
        await release.wait()
        return _ok_execute_response()

    traced = trace_tool("execute_sql_query")(slow_handler)
    first_task = asyncio.create_task(traced(tenant_id=101))
    await started.wait()

    try:
        second_raw = await traced(tenant_id=101)
        second = json.loads(second_raw)

        assert second["error"]["category"] == "limit_exceeded"
        assert second["error"]["code"] == "TENANT_TOOL_CONCURRENCY_LIMIT_EXCEEDED"
        assert second["error"]["retry_after_seconds"] == pytest.approx(1.25, rel=0, abs=1e-6)
        assert second["error"]["retryable"] is True
        recent = get_audit_event_buffer().list_recent(limit=1)
        assert recent[0]["event_type"] == AuditEventType.TENANT_CONCURRENCY_BLOCK.value
        assert recent[0]["tenant_id"] == 101
    finally:
        release.set()
        await first_task


@pytest.mark.asyncio
async def test_different_tenants_do_not_interfere(monkeypatch):
    """Different tenants should not block each other at the MCP tool boundary."""
    monkeypatch.setenv("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", "1")
    reset_mcp_tool_tenant_limiter()

    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_handler(tenant_id: int):
        if tenant_id == 1:
            started.set()
            await release.wait()
        return _ok_execute_response()

    traced = trace_tool("execute_sql_query")(slow_handler)
    first_task = asyncio.create_task(traced(tenant_id=1))
    await started.wait()

    try:
        second = await traced(tenant_id=2)
        assert isinstance(second, dict)
        assert second.get("error") is None
    finally:
        release.set()
        await first_task


@pytest.mark.asyncio
async def test_tenant_slot_released_when_call_is_cancelled(monkeypatch):
    """Cancellation should release the tenant semaphore slot."""
    monkeypatch.setenv("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", "1")
    reset_mcp_tool_tenant_limiter()

    started = asyncio.Event()
    first_call = {"pending": True}

    async def cancellable_handler(tenant_id: int):
        _ = tenant_id
        if first_call["pending"]:
            first_call["pending"] = False
            started.set()
            await asyncio.sleep(30)
        return _ok_execute_response()

    traced = trace_tool("execute_sql_query")(cancellable_handler)

    task = asyncio.create_task(traced(tenant_id=404))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    follow_up = await traced(tenant_id=404)
    assert isinstance(follow_up, dict)
    assert follow_up.get("error") is None


@pytest.mark.asyncio
async def test_mcp_rate_smoothing_rejects_rapid_burst(monkeypatch):
    """Tool-level limiter should throttle rapid bursts based on token state."""
    monkeypatch.setenv("MCP_TENANT_MAX_CONCURRENT_TOOL_CALLS", "4")
    monkeypatch.setenv("MCP_TENANT_RATE_BURST_CAPACITY", "1")
    monkeypatch.setenv("MCP_TENANT_RATE_REFILL_PER_SECOND", "1")
    monkeypatch.setenv("MCP_TENANT_LIMIT_RETRY_AFTER_SECONDS", "0.5")
    reset_mcp_tool_tenant_limiter()

    async def quick_handler(tenant_id: int):
        _ = tenant_id
        return _ok_execute_response()

    traced = trace_tool("execute_sql_query")(quick_handler)

    first = await traced(tenant_id=77)
    assert isinstance(first, dict)
    assert first.get("error") is None

    second_raw = await traced(tenant_id=77)
    second = json.loads(second_raw) if isinstance(second_raw, str) else second_raw
    assert second["error"]["code"] == "TENANT_TOOL_CONCURRENCY_LIMIT_EXCEEDED"
    assert second["error"]["retry_after_seconds"] > 0

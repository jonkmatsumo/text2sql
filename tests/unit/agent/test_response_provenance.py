"""Tests for response provenance metadata."""

from unittest.mock import AsyncMock

import pytest

from agent_service import app as app_module
from agent_service.app import AgentRunRequest, run_agent


@pytest.mark.asyncio
async def test_response_omits_provenance_by_default(monkeypatch):
    """Ensure provenance is omitted when the flag is disabled."""
    monkeypatch.delenv("AGENT_RESPONSE_PROVENANCE_METADATA", raising=False)
    monkeypatch.setattr(app_module.telemetry, "get_current_trace_id", lambda: None)

    state = {
        "messages": [],
        "current_sql": "SELECT 1",
        "query_result": [{"id": 1}],
        "error": None,
    }
    monkeypatch.setattr(app_module, "run_agent_with_tracing", AsyncMock(return_value=state))

    response = await run_agent(AgentRunRequest(question="q", tenant_id=1))

    assert response.provenance is None


@pytest.mark.asyncio
async def test_response_includes_provenance_when_enabled(monkeypatch):
    """Ensure provenance is included when the flag is enabled."""
    monkeypatch.setenv("AGENT_RESPONSE_PROVENANCE_METADATA", "true")
    monkeypatch.setenv("QUERY_TARGET_BACKEND", "bigquery")
    monkeypatch.setattr(app_module.telemetry, "get_current_trace_id", lambda: None)

    state = {
        "messages": [],
        "current_sql": "SELECT 1",
        "query_result": [{"id": 1}, {"id": 2}],
        "result_rows_returned": 2,
        "result_is_truncated": False,
        "result_is_limited": True,
        "schema_snapshot_id": "fp-abc123",
        "error": None,
    }
    monkeypatch.setattr(app_module, "run_agent_with_tracing", AsyncMock(return_value=state))

    response = await run_agent(AgentRunRequest(question="q", tenant_id=1))

    assert response.provenance is not None
    assert response.provenance["provider"] == "bigquery"
    assert response.provenance["schema_snapshot_id"] == "fp-abc123"
    assert response.provenance["rows_returned"] == 2
    assert response.provenance["is_limited"] is True
    assert "executed_at" in response.provenance

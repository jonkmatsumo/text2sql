"""Tests for execute_sql_query capability validation."""

import json
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from dal.capabilities import BackendCapabilities
from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_tool_rejects_include_columns_when_unsupported():
    """Reject include_columns requests when capability is unsupported."""
    caps = BackendCapabilities(execution_model="sync", supports_column_metadata=False)
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert result["required_capability"] == "column_metadata"
    assert result["capability_required"] == "column_metadata"
    assert result["capability_supported"] is False
    assert result["fallback_applied"] is False
    assert result["fallback_mode"] == "disable_column_metadata"


@pytest.mark.asyncio
async def test_execute_tool_rejects_async_timeout_without_cancel():
    """Reject async timeout requests when cancel is unsupported."""
    caps = BackendCapabilities(execution_model="async", supports_cancel=False)
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="snowflake",
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1, timeout_seconds=1.0)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert result["required_capability"] == "async_cancel"
    assert result["capability_required"] == "async_cancel"
    assert result["capability_supported"] is False
    assert result["fallback_applied"] is False
    assert result["fallback_mode"] == "none"


@pytest.mark.asyncio
async def test_tool_returns_classified_unsupported_capability_error():
    """Ensure unsupported capability errors are structured."""
    caps = BackendCapabilities(execution_model="sync", supports_column_metadata=False)
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert "error" in result


@pytest.mark.asyncio
async def test_execute_tool_applies_column_metadata_fallback_when_enabled(monkeypatch):
    """Apply mode should explicitly disable include_columns when unsupported."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "apply")
    caps = BackendCapabilities(execution_model="sync", supports_column_metadata=False)

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

    result = json.loads(payload)
    assert result["rows"] == [{"id": 1}]
    assert result["metadata"]["fallback_applied"] is True
    assert result["metadata"]["fallback_mode"] == "disable_column_metadata"
    assert result["metadata"]["capability_required"] == "column_metadata"

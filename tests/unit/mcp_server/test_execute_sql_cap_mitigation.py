"""Tests for provider-cap mitigation metadata and behavior."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_provider_cap_metadata_detected_without_mitigation():
    """Provider-cap truncation should be disclosed even when mitigation is off."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )

    class _Conn:
        last_truncated = True
        last_truncated_reason = "PROVIDER_CAP"

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
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1)

    result = json.loads(payload)
    assert result["metadata"]["partial_reason"] == "PROVIDER_CAP"
    assert result["metadata"]["cap_detected"] is True
    assert result["metadata"]["cap_mitigation_applied"] is False
    assert result["metadata"]["cap_mitigation_mode"] == "none"


@pytest.mark.asyncio
async def test_provider_cap_safe_mode_uses_limited_view(monkeypatch):
    """Safe mitigation mode should disclose an explicit limited view fallback."""
    monkeypatch.setenv("AGENT_PROVIDER_CAP_MITIGATION", "safe")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )

    class _Conn:
        last_truncated = True
        last_truncated_reason = "PROVIDER_CAP"

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
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1)

    result = json.loads(payload)
    assert result["metadata"]["cap_detected"] is True
    assert result["metadata"]["cap_mitigation_applied"] is True
    assert result["metadata"]["cap_mitigation_mode"] == "limited_view"


@pytest.mark.asyncio
async def test_provider_cap_safe_mode_prefers_pagination_continuation(monkeypatch):
    """When pagination is available, mitigation should prefer continuation mode."""
    monkeypatch.setenv("AGENT_PROVIDER_CAP_MITIGATION", "safe")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _Conn:
        last_truncated = True
        last_truncated_reason = "PROVIDER_CAP"

        async def fetch_page(self, sql, page_token, page_size, *params):
            _ = sql, page_token, page_size, params
            return [{"id": 1}], "next-token"

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
    ):
        payload = await handler("SELECT 1", tenant_id=1, page_token="token-1", page_size=5)

    result = json.loads(payload)
    assert result["metadata"]["next_page_token"] == "next-token"
    assert result["metadata"]["cap_detected"] is True
    assert result["metadata"]["cap_mitigation_applied"] is True
    assert result["metadata"]["cap_mitigation_mode"] == "pagination_continuation"

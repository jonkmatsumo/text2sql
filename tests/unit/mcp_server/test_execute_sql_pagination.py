"""Tests for execute_sql_query pagination handling."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_rejects_unsupported(monkeypatch):
    """Pagination options should be rejected when unsupported."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "off")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )
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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" in result
    error_obj = result["error"]
    assert error_obj["category"] in ["unknown", "unsupported_capability"]
    if error_obj["category"] == "unsupported_capability":
        details = error_obj.get("details_safe") or {}
        required = details.get("required_capability") or details.get("capability_required")
        assert required == "pagination"


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_suggests_fallback(monkeypatch):
    """Suggest mode should disclose fallback without changing behavior."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "suggest")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )
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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token",
            page_size=10,
        )

    result = json.loads(payload)
    error_obj = result["error"]
    assert error_obj["category"] == "unsupported_capability"
    details = error_obj.get("details_safe") or {}
    assert details["fallback_applied"] is False
    assert details["fallback_mode"] == "force_limited_results"


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_metadata():
    """Pagination metadata should surface in response envelope."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _Conn:
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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["metadata"]["next_page_token"] == "next-token"
    assert result["metadata"]["page_size"] == 10
    assert result["metadata"]["page_items_returned"] == 1


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_bounds():
    """Page size bounds should be enforced."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _Conn:
        async def fetch_page(self, sql, page_token, page_size, *params):
            _ = sql, page_token, page_size, params
            return [{"id": 1}], None

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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token",
            page_size=0,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"] == "execution_pagination_page_size_invalid"
    )

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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token",
            page_size=5000,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_page_size_exceeds_max_rows"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_backcompat():
    """Legacy calls without pagination should remain compatible."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 2}]

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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
        )

    result = json.loads(payload)
    assert result["rows"] == [{"id": 2}]
    # next_page_token might be absent if None/excluded
    assert result["metadata"].get("next_page_token") is None
    assert result["metadata"].get("page_size") is None
    assert result["metadata"].get("page_items_returned") in (None, 1)


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_page_token_length_bounded(monkeypatch):
    """Oversized page tokens should fail closed with deterministic classification."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _Conn:
        async def fetch_page(self, sql, page_token, page_size, *params):
            _ = sql, page_token, page_size, params
            return [{"id": 1}], None

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    monkeypatch.setenv("EXECUTION_PAGINATION_TOKEN_MAX_LENGTH", "8")

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
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            page_token="token-exceeds-limit",
            page_size=1,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"] == "execution_pagination_page_token_too_long"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_apply_mode_forces_limited_results(monkeypatch):
    """Apply mode should return an explicit limited view when pagination is unsupported."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "apply")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}, {"id": 2}]

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
        payload = await handler("SELECT 1", tenant_id=1, page_size=1)

    result = json.loads(payload)
    assert result["rows"] == [{"id": 1}]
    assert result["metadata"]["is_truncated"] is True
    from common.constants.reason_codes import PayloadTruncationReason

    assert result["metadata"]["partial_reason"] == PayloadTruncationReason.PROVIDER_CAP.value
    assert result["metadata"]["capability_required"] == "pagination"
    assert result["metadata"]["capability_supported"] is False
    assert result["metadata"]["fallback_applied"] is True
    assert result["metadata"]["fallback_mode"] == "force_limited_results"

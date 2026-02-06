"""Tests for execute_sql_query pagination handling."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_rejects_unsupported():
    """Pagination options should be rejected when unsupported."""
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
    assert result["error_category"] == "unsupported_capability"
    assert result["required_capability"] == "pagination"
    assert result["provider"] == "postgres"


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
    assert result["error_category"] == "invalid_request"

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
    assert result["metadata"]["page_size"] == 1000


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
    assert result["metadata"]["next_page_token"] is None

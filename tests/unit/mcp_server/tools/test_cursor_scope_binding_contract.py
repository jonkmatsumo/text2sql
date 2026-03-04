"""Regression coverage for pagination cursor scope binding semantics."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dal.execution_resource_limits import ExecutionResourceLimits
from dal.offset_pagination import (
    build_cursor_query_fingerprint,
    build_query_fingerprint,
    encode_offset_pagination_token,
)
from dal.pagination_cursor import PAGINATION_CURSOR_SCOPE_MISMATCH, build_cursor_scope_fingerprint
from mcp_server.tools.execute_sql_query import handler

pytestmark = pytest.mark.pagination

_TEST_SECRET = "test-pagination-secret-for-unit-tests-2026"
_BUDGET_SNAPSHOT = {
    "max_total_rows": 1000,
    "max_total_bytes": 1_000_000,
    "max_total_duration_ms": 60_000,
    "consumed_rows": 0,
    "consumed_bytes": 0,
    "consumed_duration_ms": 0,
}


def _offset_caps() -> SimpleNamespace:
    return SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )


class _Conn:
    async def fetch(self, _sql, *_params):
        return [{"id": 1}, {"id": 2}, {"id": 3}]


@asynccontextmanager
async def _conn_ctx():
    yield _Conn()


@pytest.mark.asyncio
async def test_scope_binding_rejects_cross_tenant_cursor_reuse():
    """Offset cursor minted for tenant A must be rejected for tenant B."""
    sql = "SELECT id FROM users ORDER BY id"
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_offset_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
    ):
        first_payload = await handler(sql, tenant_id=1, page_size=2)
        first = json.loads(first_payload)
        token = first["metadata"]["next_page_token"]
        assert token

        second_payload = await handler(sql, tenant_id=2, page_size=2, page_token=token)

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert second["error"]["error_code"] == "VALIDATION_ERROR"
    assert second["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH


@pytest.mark.asyncio
async def test_scope_binding_rejects_cross_query_cursor_reuse():
    """Offset cursor minted for query Q1 must not be reusable for query Q2."""
    sql_one = "SELECT id FROM users ORDER BY id"
    sql_two = "SELECT id FROM users WHERE active = true ORDER BY id"
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_offset_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
    ):
        first_payload = await handler(sql_one, tenant_id=1, page_size=2)
        first = json.loads(first_payload)
        token = first["metadata"]["next_page_token"]
        assert token

        second_payload = await handler(sql_two, tenant_id=1, page_size=2, page_token=token)

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert second["error"]["error_code"] == "VALIDATION_ERROR"
    assert second["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH


@pytest.mark.asyncio
async def test_scope_binding_rejects_cross_provider_cursor_reuse():
    """Offset cursor minted for provider X must fail under provider Y."""
    sql = "SELECT id FROM users ORDER BY id"
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_offset_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
    ):
        first_payload = await handler(sql, tenant_id=1, page_size=2)
        first = json.loads(first_payload)
        token = first["metadata"]["next_page_token"]
        assert token

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_offset_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="mysql",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
    ):
        second_payload = await handler(sql, tenant_id=1, page_size=2, page_token=token)

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert second["error"]["error_code"] == "VALIDATION_ERROR"
    assert second["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH


@pytest.mark.asyncio
async def test_scope_binding_rejects_cross_mode_cursor_reuse():
    """Offset decode must reject tokens carrying keyset scope fingerprints."""
    sql = "SELECT id FROM users ORDER BY id"
    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql=sql,
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    keyset_query_fp = build_cursor_query_fingerprint(
        sql=sql,
        provider="postgres",
        pagination_mode="keyset",
    )
    scope_fp_for_keyset = build_cursor_scope_fingerprint(
        tenant_id=1,
        provider_name="postgres",
        provider_mode="single_backend",
        tenant_enforcement_mode="",
        pagination_mode="keyset",
        query_fingerprint=keyset_query_fp,
    )
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint,
        secret=_TEST_SECRET,
        scope_fp=scope_fp_for_keyset,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=_offset_caps(),
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
    ):
        payload = await handler(sql, tenant_id=1, page_size=2, page_token=token)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH

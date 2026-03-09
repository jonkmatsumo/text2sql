"""Regression coverage for pagination cursor scope binding semantics."""

from __future__ import annotations

import base64
import hashlib
import hmac
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
from dal.pagination_session import (
    PAGINATION_SESSION_MISSING,
    PAGINATION_SESSION_SCOPE_MISMATCH,
    create_pagination_session,
    get_default_pagination_session_registry,
)
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


def _default_policy_snapshot_fp() -> str:
    payload = {
        "tenant_enforcement_mode": "rls_session",
        "tenant_rewrite_outcome": "APPLIED",
        "tenant_rewrite_reason_code": None,
    }
    digest = hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return digest[:32]


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


def _decode_offset_wrapper(token: str) -> dict[str, object]:
    padded = token + "=" * (-len(token) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))


def _encode_offset_wrapper(wrapper: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(
        json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")


def _register_offset_pagination_session(*, tenant_id: int = 1, query_scope_fp: str) -> str:
    session = create_pagination_session(
        tenant_id=str(tenant_id),
        provider_name="postgres",
        pagination_mode="offset",
        query_scope_fp=query_scope_fp,
        policy_snapshot_fp=_default_policy_snapshot_fp(),
        revocation_epoch=0,
    )
    get_default_pagination_session_registry().put(session)
    return session.session_id


def _build_offset_cursor_bindings(*, sql: str, tenant_id: int) -> tuple[str, str]:
    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql=sql,
        params=[],
        tenant_id=tenant_id,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    query_fp = build_cursor_query_fingerprint(
        sql=sql,
        provider="postgres",
        pagination_mode="offset",
    )
    scope_fp = build_cursor_scope_fingerprint(
        tenant_id=tenant_id,
        provider_name="postgres",
        provider_mode="single_backend",
        tenant_enforcement_mode=None,
        pagination_mode="offset",
        query_fingerprint=query_fp,
    )
    return fingerprint, scope_fp


@pytest.mark.asyncio
async def test_offset_first_page_mints_session_and_embeds_it_in_cursor(monkeypatch):
    """First offset page should mint a server session and bind it into cursor payload."""
    sql = "SELECT id FROM users ORDER BY id"
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _TEST_SECRET)
    get_default_pagination_session_registry().clear()
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
        payload = await handler(sql, tenant_id=1, page_size=2)

    result = json.loads(payload)
    assert "error" not in result
    session_id = result["metadata"].get("pagination_session_id")
    assert isinstance(session_id, str) and session_id
    token = result["metadata"].get("next_page_token")
    assert isinstance(token, str) and token
    wrapper = _decode_offset_wrapper(token)
    assert wrapper["p"]["pagination_session_id"] == session_id
    assert get_default_pagination_session_registry().get(session_id) is not None


@pytest.mark.asyncio
async def test_offset_second_page_validates_same_session_id(monkeypatch):
    """Continuation requests should retain and validate the same pagination session id."""
    sql = "SELECT id FROM users ORDER BY id"
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _TEST_SECRET)
    get_default_pagination_session_registry().clear()
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
        session_id = first["metadata"].get("pagination_session_id")
        token = first["metadata"].get("next_page_token")
        assert isinstance(session_id, str) and session_id
        assert isinstance(token, str) and token

        second_payload = await handler(sql, tenant_id=1, page_size=2, page_token=token)

    second = json.loads(second_payload)
    assert "error" not in second
    assert second["metadata"].get("pagination_session_id") == session_id
    next_token = second["metadata"].get("next_page_token")
    assert isinstance(next_token, str) and next_token
    wrapper = _decode_offset_wrapper(next_token)
    assert wrapper["p"]["pagination_session_id"] == session_id


@pytest.mark.asyncio
async def test_offset_missing_session_id_in_cursor_rejected_fail_closed(monkeypatch):
    """Continuation cursor missing session binding should be rejected fail closed."""
    sql = "SELECT id FROM users ORDER BY id"
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _TEST_SECRET)
    get_default_pagination_session_registry().clear()
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
        wrapper = _decode_offset_wrapper(token)
        payload = dict(wrapper["p"])
        payload.pop("pagination_session_id", None)
        payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        wrapper["p"] = payload
        wrapper["s"] = hmac.new(
            _TEST_SECRET.encode("utf-8"), payload_bytes, digestmod=hashlib.sha256
        ).hexdigest()
        tampered = _encode_offset_wrapper(wrapper)

        second_payload = await handler(sql, tenant_id=1, page_size=2, page_token=tampered)

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert second["error"]["error_code"] == "VALIDATION_ERROR"
    assert second["error"]["details_safe"]["reason_code"] == PAGINATION_SESSION_MISSING
    assert (
        second["metadata"].get("pagination.cursor.decode_reason_code") == PAGINATION_SESSION_MISSING
    )


@pytest.mark.asyncio
async def test_offset_session_scope_mismatch_rejects_cross_tenant_when_cursor_scope_matches(
    monkeypatch,
):
    """Session tenant binding must reject cross-tenant reuse even with matching cursor scope."""
    sql = "SELECT id FROM users ORDER BY id"
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _TEST_SECRET)
    get_default_pagination_session_registry().clear()
    fingerprint, scope_fp = _build_offset_cursor_bindings(sql=sql, tenant_id=2)
    session_id = _register_offset_pagination_session(tenant_id=1, query_scope_fp=scope_fp)
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint,
        secret=_TEST_SECRET,
        scope_fp=scope_fp,
        budget_snapshot=_BUDGET_SNAPSHOT,
        pagination_session_id=session_id,
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
        payload = await handler(sql, tenant_id=2, page_size=2, page_token=token)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_SESSION_SCOPE_MISMATCH
    metadata = result["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_SESSION_SCOPE_MISMATCH


@pytest.mark.asyncio
async def test_offset_session_scope_mismatch_rejects_cross_query_session_reuse(monkeypatch):
    """Session query-scope binding must reject reuse against a different SQL fingerprint."""
    sql_one = "SELECT id FROM users ORDER BY id"
    sql_two = "SELECT id FROM users WHERE active = true ORDER BY id"
    monkeypatch.setenv("PAGINATION_CURSOR_HMAC_SECRET", _TEST_SECRET)
    get_default_pagination_session_registry().clear()
    _, scope_fp_one = _build_offset_cursor_bindings(sql=sql_one, tenant_id=1)
    fingerprint_two, scope_fp_two = _build_offset_cursor_bindings(sql=sql_two, tenant_id=1)
    session_id = _register_offset_pagination_session(tenant_id=1, query_scope_fp=scope_fp_one)
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint_two,
        secret=_TEST_SECRET,
        scope_fp=scope_fp_two,
        budget_snapshot=_BUDGET_SNAPSHOT,
        pagination_session_id=session_id,
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
        payload = await handler(sql_two, tenant_id=1, page_size=2, page_token=token)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_SESSION_SCOPE_MISMATCH
    metadata = result["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_SESSION_SCOPE_MISMATCH


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
        assert first["metadata"].get("pagination.cursor.scope_bound") is True
        assert first["metadata"].get("pagination.cursor.scope_mismatch") is False
        token = first["metadata"]["next_page_token"]
        assert token

        second_payload = await handler(sql, tenant_id=2, page_size=2, page_token=token)

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert second["error"]["error_code"] == "VALIDATION_ERROR"
    assert second["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH
    metadata = second["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SCOPE_MISMATCH


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
    metadata = second["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SCOPE_MISMATCH


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
    metadata = second["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SCOPE_MISMATCH


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
        pagination_session_id=_register_offset_pagination_session(
            tenant_id=1, query_scope_fp=scope_fp_for_keyset
        ),
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
        patch("mcp_server.tools.execute_sql_query.mcp_metrics.add_counter") as add_counter,
    ):
        payload = await handler(sql, tenant_id=1, page_size=2, page_token=token)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SCOPE_MISMATCH
    metadata = result["metadata"]
    assert metadata.get("pagination.cursor.scope_bound") is True
    assert metadata.get("pagination.cursor.scope_mismatch") is True
    assert metadata.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SCOPE_MISMATCH
    matching_calls = [
        call
        for call in add_counter.call_args_list
        if call.args and call.args[0] == "pagination.cursor.scope_binding_failure_total"
    ]
    assert matching_calls
    attrs = matching_calls[-1].kwargs.get("attributes", {})
    assert attrs.get("reason_code") == PAGINATION_CURSOR_SCOPE_MISMATCH

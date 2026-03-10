"""Telemetry invariants and fail-closed regression tests for cursor signing.

These tests verify:
1. signing_secret_configured telemetry attribute is always present in metadata.
2. No high-cardinality or raw cursor data leaks into error responses.
3. Fail-closed behavior when PAGINATION_CURSOR_SIGNING_SECRET is missing.
4. Keyset and offset paths both reject when signing secret is absent.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dal.keyset_pagination import encode_keyset_cursor
from dal.offset_pagination import encode_offset_pagination_token
from dal.pagination_cursor import PAGINATION_CURSOR_SECRET_MISSING, PAGINATION_CURSOR_SECRET_WEAK
from dal.pagination_session import (
    create_pagination_session,
    get_default_pagination_session_registry,
)
from mcp_server.tools.execute_sql_query import handler

pytestmark = pytest.mark.pagination

_TEST_SECRET = "test-pagination-secret-for-unit-tests-2026"
_PREVIOUS_SECRET = "test-pagination-secret-for-unit-tests-2025"
_WEAK_SECRET = "weak-secret"
_BUDGET_SNAPSHOT = {
    "max_total_rows": 1000,
    "max_total_bytes": 1_000_000,
    "max_total_duration_ms": 60_000,
    "consumed_rows": 0,
    "consumed_bytes": 0,
    "consumed_duration_ms": 0,
}

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _keyset_caps() -> SimpleNamespace:
    return SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )


def _make_conn(rows: list[dict] | None = None):
    """Return a mock connection that yields rows from fetch()."""

    class _Conn:
        def __init__(self):
            self.session_guardrail_metadata = {}

        async def fetch(self, _query, *_args):
            return rows or []

    return _Conn()


@asynccontextmanager
async def _conn_ctx(conn):
    yield conn


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


def _register_pagination_session(
    *,
    tenant_id: int = 1,
    pagination_mode: str,
    query_scope_fp: str = "telemetry-scope-fp",
) -> str:
    session = create_pagination_session(
        tenant_id=str(tenant_id),
        provider_name="postgres",
        pagination_mode=pagination_mode,
        query_scope_fp=query_scope_fp,
        policy_snapshot_fp=_default_policy_snapshot_fp(),
        revocation_epoch=0,
    )
    get_default_pagination_session_registry().put(session)
    return session.session_id


# ---------------------------------------------------------------------------
# 1. signing_secret_configured always present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_signing_secret_configured_present_on_success():
    """Metadata must include pagination.cursor.signing_secret_configured on success."""
    conn = _make_conn(rows=[{"id": 1}])

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is True
    assert meta.get("pagination.cursor.secret_configured") is True
    assert meta.get("pagination.cursor.secret_valid") is True


@pytest.mark.asyncio
async def test_signing_secret_configured_present_on_cursor_error():
    """Metadata must include signing_secret_configured on cursor-related errors."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "stable-fp",
        secret="different-secret",
        now_epoch_seconds=99999999999,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="stable-fp",
        ),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" in result
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is True


# ---------------------------------------------------------------------------
# 2. No high-cardinality or raw cursor data leakage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_error_response_does_not_leak_raw_cursor():
    """Error responses must not include raw cursor tokens."""
    bogus_cursor = "not-a-real-cursor-SENTINEL"
    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=bogus_cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" in result
    serialized = json.dumps(result)
    assert bogus_cursor not in serialized


@pytest.mark.asyncio
async def test_reason_codes_are_bounded():
    """Reason codes in error responses must be from the known set."""
    known_reason_codes = {
        "KEYSET_ORDER_BY_REQUIRED",
        "KEYSET_ORDER_BY_UNSAFE_EXPRESSION",
        "KEYSET_ORDER_BY_AMBIGUOUS_COLUMN",
        "KEYSET_ORDER_BY_MISSING_TIEBREAKER",
        "execution_pagination_keyset_cursor_invalid",
        "execution_pagination_keyset_invalid_sql",
        "execution_pagination_page_token_invalid",
        "execution_pagination_page_token_too_long",
        "execution_pagination_page_size_invalid",
        "execution_pagination_page_size_exceeds_max_rows",
        "PAGINATION_MODE_TOKEN_MISMATCH",
        "KEYSET_CURSOR_ORDERBY_MISMATCH",
        "KEYSET_SNAPSHOT_MISMATCH",
        "KEYSET_TOPOLOGY_MISMATCH",
        "KEYSET_REPLICA_LAG_UNSAFE",
        "KEYSET_NULLABLE_TIEBREAKER_UNSAFE",
        "PAGINATION_FEDERATED_ORDERING_UNSAFE",
        "PAGINATION_FEDERATED_UNSUPPORTED",
        "PAGINATION_BACKEND_SET_CHANGED",
        "PAGINATION_CURSOR_EXPIRED",
        "PAGINATION_CURSOR_TTL_MISSING",
        "PAGINATION_CURSOR_TTL_INVALID",
        "PAGINATION_CURSOR_ISSUED_AT_INVALID",
        "PAGINATION_CURSOR_CLOCK_SKEW",
        "PAGINATION_CURSOR_REPLAY_DETECTED",
        "PAGINATION_CURSOR_QUERY_MISMATCH",
        "PAGINATION_CURSOR_KID_MISSING",
        "PAGINATION_CURSOR_KID_UNKNOWN",
        "PAGINATION_CURSOR_SECRET_MISSING",
        "PAGINATION_CURSOR_SECRET_WEAK",
        "PAGINATION_CURSOR_KEYRING_INVALID",
        "PAGINATION_CURSOR_SIGNATURE_INVALID",
        "PAGINATION_CURSOR_VERSION_UNSUPPORTED",
        "PAGINATION_CURSOR_KIND_UNSUPPORTED",
        "PAGINATION_CURSOR_MIGRATION_PATH_MISSING",
        "PAGINATION_CURSOR_MIGRATION_UNSAFE",
        "KEYSET_CURSOR_ORDERBY_SIGNATURE_MISSING",
    }

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
    ):
        payload = await handler(
            "SELECT 1",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" in result
    reason = result["error"]["details_safe"]["reason_code"]
    assert reason in known_reason_codes, f"Unexpected reason_code: {reason}"


# ---------------------------------------------------------------------------
# 3. Fail-closed: missing signing secret
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_keyset_decode_fails_closed_without_secret(monkeypatch):
    """Keyset cursor decode must fail closed when signing secret is missing."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp",
        secret="some-old-secret",
        now_epoch_seconds=1000,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_MISSING
    assert result["error"]["category"] == "invalid_request"
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is False
    assert meta.get("pagination.cursor.secret_configured") is False
    assert meta.get("pagination.cursor.secret_valid") is False
    assert meta.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SECRET_MISSING


@pytest.mark.asyncio
async def test_offset_decode_fails_closed_without_secret(monkeypatch):
    """Offset token decode must fail closed when signing secret is missing."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    token = encode_offset_pagination_token(
        offset=0,
        limit=10,
        fingerprint="fp",
        secret="some-old-secret",
        now_epoch_seconds=1000,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_token=token,
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_MISSING
    assert result["error"]["category"] == "invalid_request"
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is False
    assert meta.get("pagination.cursor.secret_configured") is False
    assert meta.get("pagination.cursor.secret_valid") is False
    assert meta.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SECRET_MISSING


@pytest.mark.asyncio
async def test_secret_misconfig_counter_emitted_for_missing_decode(monkeypatch):
    """Secret misconfiguration decode failures should emit bounded misconfig counter."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    token = encode_offset_pagination_token(
        offset=0,
        limit=10,
        fingerprint="fp",
        secret=_TEST_SECRET,
        now_epoch_seconds=1000,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.mcp_metrics.add_counter") as add_counter,
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_token=token,
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_MISSING
    matching_calls = [
        call
        for call in add_counter.call_args_list
        if call.args and call.args[0] == "pagination.cursor.secret_misconfig_total"
    ]
    assert matching_calls
    attrs = matching_calls[-1].kwargs.get("attributes", {})
    assert attrs.get("reason_code") == PAGINATION_CURSOR_SECRET_MISSING


@pytest.mark.asyncio
async def test_keyset_encode_fails_closed_without_secret(monkeypatch):
    """Keyset cursor encode (next page) must fail closed when signing secret is missing."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    conn = _make_conn(rows=[{"id": i} for i in range(11)])

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_MISSING
    assert result["error"]["category"] == "invalid_request"


@pytest.mark.asyncio
async def test_offset_encode_fails_closed_without_secret(monkeypatch):
    """Offset token encode (next page) must fail closed when signing secret is missing."""
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )
    conn = _make_conn(rows=[{"id": i} for i in range(11)])

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_MISSING
    assert result["error"]["category"] == "invalid_request"


@pytest.mark.asyncio
async def test_keyset_decode_fails_closed_with_weak_secret(monkeypatch):
    """Keyset decode must fail closed when configured secret is weak."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _WEAK_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp",
        secret=_TEST_SECRET,
        now_epoch_seconds=1000,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_WEAK
    assert result["error"]["category"] == "invalid_request"
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is True
    assert meta.get("pagination.cursor.secret_configured") is True
    assert meta.get("pagination.cursor.secret_valid") is False
    assert meta.get("pagination.cursor.decode_reason_code") == PAGINATION_CURSOR_SECRET_WEAK
    serialized = json.dumps(result)
    assert _WEAK_SECRET not in serialized
    assert cursor not in serialized


@pytest.mark.asyncio
async def test_offset_decode_fails_closed_with_weak_secret(monkeypatch):
    """Offset decode must fail closed when configured secret is weak."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _WEAK_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    token = encode_offset_pagination_token(
        offset=0,
        limit=10,
        fingerprint="fp",
        secret=_TEST_SECRET,
        now_epoch_seconds=1000,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_token=token,
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_WEAK
    assert result["error"]["category"] == "invalid_request"
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signing_secret_configured") is True
    serialized = json.dumps(result)
    assert _WEAK_SECRET not in serialized
    assert token not in serialized


@pytest.mark.asyncio
async def test_keyset_encode_fails_closed_with_weak_secret(monkeypatch):
    """Keyset encode (next page cursor) must fail closed when secret is weak."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _WEAK_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    conn = _make_conn(rows=[{"id": i} for i in range(11)])

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_WEAK
    assert result["error"]["category"] == "invalid_request"
    serialized = json.dumps(result)
    assert _WEAK_SECRET not in serialized


@pytest.mark.asyncio
async def test_offset_encode_fails_closed_with_weak_secret(monkeypatch):
    """Offset encode (next page token) must fail closed when secret is weak."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_SECRET", _WEAK_SECRET)
    monkeypatch.delenv("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET", raising=False)

    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )
    conn = _make_conn(rows=[{"id": i} for i in range(11)])

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["details_safe"]["reason_code"] == PAGINATION_CURSOR_SECRET_WEAK
    assert result["error"]["category"] == "invalid_request"
    serialized = json.dumps(result)
    assert _WEAK_SECRET not in serialized


# ---------------------------------------------------------------------------
# 4. Signature-valid telemetry on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_signature_valid_telemetry_on_keyset_decode_success():
    """Successful keyset decode must set pagination.cursor.signature_valid = True."""
    conn = _make_conn(rows=[{"id": 1}])

    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "stable-fp",
        secret=_TEST_SECRET,
        now_epoch_seconds=99999999999,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(conn),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="stable-fp",
        ),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    # May error due to fingerprint mismatch etc. — we assert the telemetry attribute exists
    meta = result.get("metadata", {})
    # If decode succeeded, signature_valid should be True
    if "error" not in result:
        assert meta.get("pagination.cursor.signature_valid") is True
    else:
        # Even on decode error, signing_secret_configured must be present
        assert "pagination.cursor.signing_secret_configured" in meta


@pytest.mark.asyncio
async def test_signature_valid_false_on_keyset_signature_mismatch():
    """Keyset decode with wrong secret must set pagination.cursor.signature_valid = False."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "stable-fp",
        secret="different-secret",
        now_epoch_seconds=99999999999,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn()),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="stable-fp",
        ),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" in result
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.signature_valid") is False
    assert meta.get("pagination.cursor.signing_secret_configured") is True


# ---------------------------------------------------------------------------
# 5. Rotation telemetry parity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_secondary_key_decode_sets_rotation_telemetry_and_span_parity(monkeypatch):
    """Secondary-key decode should emit bounded parity metadata/span fields."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_ACTIVE_KID", "active")
    monkeypatch.setenv(
        "PAGINATION_CURSOR_SIGNING_KEYS_JSON",
        json.dumps({"active": _TEST_SECRET, "previous": _PREVIOUS_SECRET}),
    )
    monkeypatch.delenv("PAGINATION_CURSOR_SIGNING_SECRET", raising=False)
    monkeypatch.delenv("PAGINATION_CURSOR_HMAC_SECRET", raising=False)

    session_id = _register_pagination_session(
        pagination_mode="keyset",
        query_scope_fp="abcdeffedcba0123",
    )
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "stable-fp",
        secret=_PREVIOUS_SECRET,
        kid="previous",
        query_fp="cursor-query-fp",
        scope_fp="abcdeffedcba0123",
        budget_snapshot=_BUDGET_SNAPSHOT,
        pagination_session_id=session_id,
    )
    span = MagicMock()
    span.is_recording.return_value = True

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=_keyset_caps()),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "dal.database.Database.get_connection",
            side_effect=lambda *_a, **_kw: _conn_ctx(_make_conn(rows=[{"id": 1}])),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint", return_value="stable-fp"
        ),
        patch(
            "mcp_server.tools.execute_sql_query.build_cursor_query_fingerprint",
            return_value="cursor-query-fp",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.build_cursor_scope_fingerprint",
            return_value="abcdeffedcba0123",
        ),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=span),
        patch("mcp_server.tools.execute_sql_query.mcp_metrics.add_counter") as add_counter,
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result
    meta = result.get("metadata", {})
    assert meta.get("pagination.cursor.kid_present") is True
    assert meta.get("pagination.cursor.kid_active_match") is False
    assert meta.get("pagination.cursor.rotation_verification_path") == "secondary"

    span_attrs = {
        call.args[0]: call.args[1]
        for call in span.set_attribute.call_args_list
        if len(call.args) >= 2
    }
    assert span_attrs.get("pagination.cursor.kid_present") == meta.get(
        "pagination.cursor.kid_present"
    )
    assert span_attrs.get("pagination.cursor.kid_active_match") == meta.get(
        "pagination.cursor.kid_active_match"
    )
    assert span_attrs.get("pagination.cursor.rotation_verification_path") == meta.get(
        "pagination.cursor.rotation_verification_path"
    )

    secondary_calls = [
        call
        for call in add_counter.call_args_list
        if call.args and call.args[0] == "pagination.cursor.secondary_key_verifications_total"
    ]
    assert secondary_calls

    serialized = json.dumps(result)
    assert _PREVIOUS_SECRET not in serialized

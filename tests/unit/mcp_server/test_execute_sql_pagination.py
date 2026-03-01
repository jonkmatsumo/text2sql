"""Tests for execute_sql_query pagination handling."""

import base64
import hashlib
import hmac
import json
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dal.execution_resource_limits import ExecutionResourceLimits
from dal.offset_pagination import (
    build_cursor_query_fingerprint,
    build_query_fingerprint,
    encode_offset_pagination_token,
)
from mcp_server.tools.execute_sql_query import handler

_TEST_SECRET = "test-pagination-secret"
_BUDGET_SNAPSHOT = {
    "max_total_rows": 1000,
    "max_total_bytes": 1_000_000,
    "max_total_duration_ms": 60_000,
    "consumed_rows": 0,
    "consumed_bytes": 0,
    "consumed_duration_ms": 0,
}


def test_build_query_fingerprint_changes_with_order_signature():
    """Order signature must be part of the pagination fingerprint context."""
    base_kwargs = {
        "sql": "SELECT id FROM users ORDER BY id ASC",
        "params": [],
        "tenant_id": 1,
        "provider": "postgres",
        "max_rows": 1000,
        "max_bytes": 1024 * 1024,
        "max_execution_ms": 30_000,
    }
    fingerprint_asc = build_query_fingerprint(
        **base_kwargs,
        order_signature='["id|asc|nulls_last"]',
    )
    fingerprint_desc = build_query_fingerprint(
        **base_kwargs,
        order_signature='["id|desc|nulls_first"]',
    )
    assert fingerprint_asc != fingerprint_desc


def test_build_cursor_query_fingerprint_normalizes_sql_variants():
    """Strict cursor query fingerprints should normalize SQL whitespace variants."""
    cursor_fp_compact = build_cursor_query_fingerprint(
        sql="SELECT id FROM users ORDER BY id ASC",
        provider="postgres",
        pagination_mode="keyset",
        order_signature='["id|asc|nulls_last"]',
    )
    cursor_fp_spaced = build_cursor_query_fingerprint(
        sql="  SELECT   id   FROM users   ORDER BY id ASC  ",
        provider="postgres",
        pagination_mode="keyset",
        order_signature='["id|asc|nulls_last"]',
    )
    assert cursor_fp_compact == cursor_fp_spaced


def test_build_cursor_query_fingerprint_changes_with_pagination_mode():
    """Pagination mode should be part of strict cursor query fingerprint identity."""
    base_kwargs = {
        "sql": "SELECT id FROM users ORDER BY id ASC",
        "provider": "postgres",
        "order_signature": '["id|asc|nulls_last"]',
    }
    assert build_cursor_query_fingerprint(**base_kwargs, pagination_mode="offset") != (
        build_cursor_query_fingerprint(**base_kwargs, pagination_mode="keyset")
    )


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_rejects_unsupported(monkeypatch):
    """Pagination options should be rejected when unsupported."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "off")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="async",
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
    assert error_obj["category"] == "invalid_request"
    details = error_obj.get("details_safe") or {}
    assert details.get("reason_code") == "execution_pagination_unsupported_provider"


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_suggests_fallback(monkeypatch):
    """Suggest mode should disclose fallback without changing behavior."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "suggest")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="async",
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
    assert error_obj["category"] == "invalid_request"
    details = error_obj.get("details_safe") or {}
    assert details["reason_code"] == "execution_pagination_unsupported_provider"


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_metadata():
    """Pagination metadata should surface in response envelope."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_args, **_kwargs: _conn_ctx(),
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
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_args, **_kwargs: _conn_ctx(),
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
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
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
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
        execution_model="async",
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
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
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_unsupported_provider"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_token_mismatch_rejected():
    """Tokens should fail when replayed against a different SQL fingerprint."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = params
            if "LIMIT 3 OFFSET 0" in sql:
                return [{"id": 1}, {"id": 2}, {"id": 3}]
            return [{"id": 4}, {"id": 5}]

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
            side_effect=lambda *_args, **_kwargs: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        first_payload = await handler("SELECT 1 AS id", tenant_id=1, page_size=2)
        first = json.loads(first_payload)
        token = first["metadata"]["next_page_token"]
        second_payload = await handler(
            "SELECT 2 AS id",
            tenant_id=1,
            page_size=2,
            page_token=token,
        )

    second = json.loads(second_payload)
    assert second["error"]["category"] == "invalid_request"
    assert (
        second["error"]["details_safe"]["reason_code"]
        == "execution_pagination_page_token_fingerprint_mismatch"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_offset_cap_enforced(monkeypatch):
    """Offset bounds should fail closed for deterministic token paging."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    monkeypatch.setenv("EXECUTION_PAGINATION_MAX_OFFSET_PAGES", "1")
    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql="SELECT 1 AS id",
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    token = encode_offset_pagination_token(
        offset=999,
        limit=2,
        fingerprint=fingerprint,
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            side_effect=lambda *_args, **_kwargs: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT 1 AS id",
            tenant_id=1,
            page_size=2,
            page_token=token,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_offset_exceeds_limit"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_rejects_expired_cursor_stable_classification():
    """Expired offset cursors should return stable invalid-request classification fields."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql="SELECT 1 AS id",
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint,
        issued_at=0,
        max_age_s=1,
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
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
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT 1 AS id",
            tenant_id=1,
            page_size=2,
            page_token=token,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_CURSOR_EXPIRED"
    assert result["metadata"]["pagination.reject_reason_code"] == "PAGINATION_CURSOR_EXPIRED"


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_query_fp_mismatch_in_strict_mode(monkeypatch):
    """Offset cursor strict binding should reject mismatched query fingerprints."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql="SELECT 1 AS id",
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint,
        issued_at=int(time.time()),
        query_fp="query-fp-a",
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )
    monkeypatch.setenv("PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT", "true")

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
        patch(
            "mcp_server.tools.execute_sql_query.build_cursor_query_fingerprint",
            return_value="query-fp-b",
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT 1 AS id",
            tenant_id=1,
            page_size=2,
            page_token=token,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["error_code"] == "VALIDATION_ERROR"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_CURSOR_QUERY_MISMATCH"
    assert result["metadata"]["pagination.reject_reason_code"] == "PAGINATION_CURSOR_QUERY_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_offset_budget_snapshot_tamper_is_rejected():
    """Offset token budget tampering should fail closed with stable reason code."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql="SELECT 1 AS id",
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    token = encode_offset_pagination_token(
        offset=0,
        limit=2,
        fingerprint=fingerprint,
        issued_at=int(time.time()),
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
    )

    padded = token + "=" * (-len(token) % 4)
    wrapper = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    wrapper["p"]["budget_snapshot"]["consumed_rows"] = 1
    inner_bytes = json.dumps(wrapper["p"], separators=(",", ":"), sort_keys=True).encode("utf-8")
    wrapper["s"] = hmac.new(
        _TEST_SECRET.encode("utf-8"), inner_bytes, digestmod=hashlib.sha256
    ).hexdigest()
    tampered_token = (
        base64.urlsafe_b64encode(
            json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        .decode("ascii")
        .rstrip("=")
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
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT 1 AS id",
            tenant_id=1,
            page_size=2,
            page_token=tampered_token,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_BUDGET_SNAPSHOT_INVALID"
    assert (
        result["metadata"]["pagination.reject_reason_code"] == "PAGINATION_BUDGET_SNAPSHOT_INVALID"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_follow_up_expired_cursor_sanitized():
    """Follow-up offset requests with expired cursors should reject without SQL/cursor leakage."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}, {"id": 2}, {"id": 3}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    sql = "SELECT 1 AS id, 'LEAK_SENTINEL_OFFSET_333' AS note"

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
            side_effect=lambda *_args, **_kwargs: _conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        first_payload = await handler(sql, tenant_id=1, page_size=2)
        first = json.loads(first_payload)
        token = first["metadata"]["next_page_token"]
        assert token

        padded = token + "=" * (-len(token) % 4)
        token_wrapper = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
        token_wrapper["p"]["issued_at"] = 0
        token_wrapper["p"]["max_age_s"] = 1
        # Re-sign inner payload after tampering
        inner_bytes = json.dumps(token_wrapper["p"], separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        token_wrapper["s"] = hmac.new(
            _TEST_SECRET.encode("utf-8"), inner_bytes, digestmod=hashlib.sha256
        ).hexdigest()
        expired_token = (
            base64.urlsafe_b64encode(
                json.dumps(token_wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
            )
            .decode("ascii")
            .rstrip("=")
        )

        second_payload = await handler(
            sql,
            tenant_id=1,
            page_size=2,
            page_token=expired_token,
        )

    second = json.loads(second_payload)
    assert second["error"]["details_safe"]["reason_code"] == "PAGINATION_CURSOR_EXPIRED"
    serialized = json.dumps(second)
    assert "LEAK_SENTINEL_OFFSET_333" not in serialized
    assert expired_token not in serialized


@pytest.mark.asyncio
async def test_execute_sql_query_offset_cursor_telemetry_parity_deterministic_clock():
    """Offset cursor metadata/spans should expose bounded deterministic validation telemetry."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    limits = ExecutionResourceLimits.from_env()
    fingerprint = build_query_fingerprint(
        sql="SELECT 1 AS id",
        params=[],
        tenant_id=1,
        provider="postgres",
        max_rows=limits.max_rows,
        max_bytes=limits.max_bytes,
        max_execution_ms=limits.max_execution_ms,
    )
    token = encode_offset_pagination_token(
        offset=0,
        limit=1,
        fingerprint=fingerprint,
        issued_at=1_000,
        max_age_s=600,
        secret=_TEST_SECRET,
        budget_snapshot=_BUDGET_SNAPSHOT,
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
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
        patch("dal.pagination_cursor.time.time", return_value=1_120),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT 1 AS id",
            tenant_id=1,
            page_size=1,
            page_token=token,
        )

    result = json.loads(payload)
    metadata = result["metadata"]
    assert "error" not in result
    assert metadata["cursor_issued_at_present"] is True
    assert metadata["cursor_age_bucket"] == "60_299"
    assert metadata["cursor_validation_outcome"] == "OK"

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call.args
        attrs[key] = value
    assert attrs["cursor_issued_at_present"] == metadata["cursor_issued_at_present"]
    assert attrs["cursor_age_bucket"] == metadata["cursor_age_bucket"]
    assert attrs["cursor_validation_outcome"] == metadata["cursor_validation_outcome"]


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_wrapper_not_marked_partial():
    """Page slicing for pagination should not set partial truncation semantics."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = params
            if "LIMIT 3 OFFSET 0" in sql:
                return [{"id": 1}, {"id": 2}, {"id": 3}]
            return []

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
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler("SELECT 1 AS id", tenant_id=1, page_size=2)

    result = json.loads(payload)
    assert result["rows"] == [{"id": 1}, {"id": 2}]
    assert result["metadata"]["partial"] is False
    assert result["metadata"]["is_truncated"] is False
    assert result["metadata"].get("partial_reason") is None
    assert result["metadata"]["next_page_token"]


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_rejects_limit_offset_sql():
    """Offset-wrapper mode should fail closed on SQL with LIMIT/OFFSET already present."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

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
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler("SELECT 1 AS id LIMIT 10", tenant_id=1, page_size=2)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_sql_contains_limit_offset"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_byte_truncation_clears_next_token(monkeypatch):
    """Byte truncation mid-page must clear continuation tokens."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = params
            if "LIMIT 3 OFFSET 0" in sql:
                return [{"blob": "x" * 64}, {"blob": "y" * 64}, {"blob": "z" * 64}]
            return []

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "90")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

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
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler("SELECT 1 AS id", tenant_id=1, page_size=2)

    result = json.loads(payload)
    assert result["metadata"]["partial_reason"] == "max_bytes"
    assert result["metadata"]["partial"] is True
    assert result["metadata"].get("next_page_token") is None


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_span_parity():
    """Pagination metadata should map deterministically to span attributes."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = params
            if "LIMIT 3 OFFSET 0" in sql:
                return [{"id": 1}, {"id": 2}, {"id": 3}]
            return []

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
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span") as mock_get_span,
    ):
        mock_span = mock_get_span.return_value
        mock_span.is_recording.return_value = True
        payload = await handler("SELECT 1 AS id", tenant_id=1, page_size=2)

    result = json.loads(payload)
    metadata = result["metadata"]
    mock_span.set_attribute.assert_any_call("db.result.page_size", metadata["page_size"])
    mock_span.set_attribute.assert_any_call(
        "db.result.page_items_returned", metadata["page_items_returned"]
    )
    mock_span.set_attribute.assert_any_call(
        "db.result.next_page_token_present", bool(metadata.get("next_page_token"))
    )
    mock_span.set_attribute.assert_any_call("db.result.partial", metadata["partial"])

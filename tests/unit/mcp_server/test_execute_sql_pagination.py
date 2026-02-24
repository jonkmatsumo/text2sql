"""Tests for execute_sql_query pagination handling."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dal.execution_resource_limits import ExecutionResourceLimits
from dal.offset_pagination import build_query_fingerprint, encode_offset_pagination_token
from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_sql_query_pagination_rejects_unsupported(monkeypatch):
    """Pagination options should be rejected when unsupported."""
    monkeypatch.setenv("AGENT_CAPABILITY_FALLBACK_MODE", "off")
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="async",
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
        execution_model="async",
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


@pytest.mark.asyncio
async def test_execute_sql_query_offset_pagination_token_mismatch_rejected():
    """Tokens should fail when replayed against a different SQL fingerprint."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
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
    token = encode_offset_pagination_token(offset=999, limit=2, fingerprint=fingerprint)

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
async def test_execute_sql_query_offset_pagination_wrapper_not_marked_partial():
    """Page slicing for pagination should not set partial truncation semantics."""
    caps = SimpleNamespace(
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=False,
        execution_model="sync",
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

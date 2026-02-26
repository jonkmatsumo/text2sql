import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_order_by_required():
    """Test that keyset pagination requires an ORDER BY clause."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
    ):
        sql = "SELECT 1"  # No ORDER BY
        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=10)

        result = json.loads(payload)
        assert result["error"]["category"] == "invalid_request"
        assert (
            result["error"]["details_safe"]["reason_code"]
            == "execution_pagination_keyset_order_by_required"
        )


@pytest.mark.asyncio
async def test_execute_sql_query_rejects_mixed_pagination_tokens():
    """Supplying both offset and keyset tokens should fail closed."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_token="offset-token",
            keyset_cursor="keyset-token",
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_MODE_TOKEN_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_rejects_keyset_mode_with_page_token():
    """Using offset token in keyset mode should be rejected."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_token="offset-token",
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_MODE_TOKEN_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_rejects_offset_mode_with_keyset_cursor():
    """Using keyset cursor in offset mode should be rejected."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            keyset_cursor="keyset-token",
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_MODE_TOKEN_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_offset_mode_with_page_token_still_allows_flow():
    """Proper offset-mode token usage should remain valid."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch_page(self, _sql, _page_token, _page_size, *_params):
                return [{"id": 1}], None

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_token="offset-token",
            page_size=1,
        )

    result = json.loads(payload)
    assert "error" not in result
    assert result["rows"] == [{"id": 1}]


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_unstable_tiebreaker_created_at_only():
    """ORDER BY created_at alone should fail stable tie-breaker validation."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_REQUIRES_STABLE_TIEBREAKER"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_allows_created_at_with_id_tiebreaker():
    """ORDER BY created_at, id should pass tie-breaker validation."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return []

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id, created_at FROM users ORDER BY created_at DESC, id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_random_tiebreaker():
    """Nondeterministic final ORDER BY key should be rejected."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC, random()",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"] == "execution_pagination_keyset_invalid_sql"
    )


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_nullable_tiebreaker_with_metadata():
    """Nullable final tie-breaker should fail when schema metadata is available."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "created_at", "nullable": False, "is_primary_key": False},
                        {"name": "id", "nullable": True, "is_primary_key": False},
                    ],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC, id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_TIEBREAKER_NULLABLE"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_order_column_missing_from_schema():
    """Missing ORDER BY schema columns should fail closed in keyset mode."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "created_at", "nullable": False, "is_primary_key": False},
                    ],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC, id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_ORDER_COLUMN_NOT_FOUND"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_allows_nullable_non_final_with_explicit_nulls_ordering():
    """Explicit NULLS ordering should allow nullable non-final ORDER BY columns."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "created_at", "nullable": True, "is_primary_key": False},
                        {"name": "id", "nullable": False, "is_primary_key": True},
                    ],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return []

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC NULLS LAST, id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_allows_composite_unique_suffix_with_schema_metadata():
    """Composite unique keys should satisfy schema-aware keyset stability checks."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "events",
                    "columns": [
                        {"name": "user_id", "nullable": False, "is_primary_key": False},
                        {"name": "created_at", "nullable": False, "is_primary_key": False},
                    ],
                    "unique_keys": [["user_id", "created_at"]],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return []

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT user_id, created_at FROM events ORDER BY user_id ASC, created_at ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_non_unique_tiebreaker_with_schema_metadata():
    """Known non-unique ORDER BY suffixes should be rejected with bounded reason codes."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "events",
                    "columns": [
                        {"name": "created_at", "nullable": False, "is_primary_key": False},
                        {"name": "id", "nullable": False, "is_primary_key": True},
                    ],
                    "unique_keys": [["id"]],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT created_at FROM events ORDER BY created_at ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_TIEBREAKER_NOT_UNIQUE"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_strict_mode_rejects_missing_schema(monkeypatch):
    """Strict schema mode should fail closed when keyset schema metadata is unavailable."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    monkeypatch.setenv("KEYSET_SCHEMA_STRICT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", side_effect=RuntimeError("missing")),
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
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_SCHEMA_REQUIRED"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_strict_mode_rejects_stale_schema(monkeypatch):
    """Strict schema mode should reject stale schema snapshots when age metadata is present."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    monkeypatch.setenv("KEYSET_SCHEMA_STRICT", "true")
    monkeypatch.setenv("KEYSET_SCHEMA_TTL_SECONDS", "60")

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "id", "nullable": False, "is_primary_key": True},
                    ],
                    "schema_age_seconds": 3600,
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
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
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_SCHEMA_STALE"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_strict_mode_disabled_keeps_fallback_without_schema(
    monkeypatch,
):
    """Disabling strict schema mode should preserve no-schema keyset fallback behavior."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    monkeypatch.setenv("KEYSET_SCHEMA_STRICT", "false")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", side_effect=RuntimeError("missing")),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return []

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    assert "error" not in result


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_schema_rejection_observability_parity():
    """Schema-aware keyset rejections should align metadata and span attributes."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True

    class _MetadataStore:
        async def get_table_definition(self, _table_name, tenant_id=None):
            _ = tenant_id
            return json.dumps(
                {
                    "table_name": "users",
                    "columns": [
                        {"name": "created_at", "nullable": False, "is_primary_key": False},
                    ],
                    "foreign_keys": [],
                }
            )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", return_value=_MetadataStore()),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY created_at DESC, id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    metadata = result["metadata"]
    reason_code = result["error"]["details_safe"]["reason_code"]

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call.args
        attrs[key] = value

    assert reason_code == "KEYSET_ORDER_COLUMN_NOT_FOUND"
    assert metadata["pagination.keyset.rejection_reason_code"] == reason_code
    assert attrs["pagination.keyset.rejection_reason_code"] == reason_code
    assert attrs["pagination.keyset.schema_used"] == metadata["pagination.keyset.schema_used"]
    assert attrs["pagination.keyset.schema_strict"] == metadata["pagination.keyset.schema_strict"]
    assert attrs["pagination.keyset.schema_stale"] == metadata["pagination.keyset.schema_stale"]


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_schema_rejection_does_not_leak_raw_sql(monkeypatch):
    """Schema-aware keyset rejections must not include raw caller SQL."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users WHERE note = 'LEAK_SENTINEL_SCHEMA_456' ORDER BY id ASC"
    monkeypatch.setenv("KEYSET_SCHEMA_STRICT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_metadata_store", side_effect=RuntimeError("missing")),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )

    result = json.loads(payload)
    serialized = json.dumps(result)
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_SCHEMA_REQUIRED"
    assert "LEAK_SENTINEL_SCHEMA_456" not in serialized
    assert sql not in serialized


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_cursor_invalid_fingerprint():
    """Test that keyset pagination rejects cursors with mismatched fingerprints."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="current-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        from dal.keyset_pagination import encode_keyset_cursor

        # Cursor from a different query/fingerprint
        cursor = encode_keyset_cursor([50], ["id|asc|nulls_last"], "old-fingerprint")

        sql = "SELECT id FROM users ORDER BY id ASC"
        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
        )

        result = json.loads(payload)
        assert result["error"]["category"] == "invalid_request"
        assert (
            result["error"]["details_safe"]["reason_code"]
            == "execution_pagination_keyset_cursor_invalid"
        )
        assert "fingerprint mismatch" in result["error"]["message"]


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_cursor_rejects_order_mismatch():
    """Cursor should be rejected when ORDER BY structure changes across requests."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="stable-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        from dal.keyset_pagination import encode_keyset_cursor

        cursor = encode_keyset_cursor([50], ["id|asc|nulls_last"], "stable-fingerprint")
        payload = await handler(
            "SELECT id FROM users ORDER BY id DESC",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_ORDER_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_cursor_allows_same_snapshot_context_reuse():
    """Keyset cursors should be reusable when snapshot context is unchanged."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="snapshot-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self, rows):
                self._rows = list(rows)
                self.snapshot_id = "snap-1"
                self.transaction_id = "tx-1"
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return list(self._rows)

        class _ConnCtx:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *_exc):
                return False

        mock_get_conn.side_effect = [
            _ConnCtx(_Conn([{"id": 1}, {"id": 2}, {"id": 3}])),
            _ConnCtx(_Conn([{"id": 3}])),
        ]

        page_one_payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        page_one_result = json.loads(page_one_payload)
        cursor = page_one_result["metadata"]["next_keyset_cursor"]
        assert cursor

        page_two_payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=2,
        )

    page_two_result = json.loads(page_two_payload)
    assert "error" not in page_two_result


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_cursor_rejects_snapshot_context_mismatch():
    """Keyset cursor must fail closed when snapshot context changes between requests."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="snapshot-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self, rows, snapshot_id):
                self._rows = list(rows)
                self.snapshot_id = snapshot_id
                self.transaction_id = "tx-1"
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return list(self._rows)

        class _ConnCtx:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *_exc):
                return False

        mock_get_conn.side_effect = [
            _ConnCtx(_Conn([{"id": 1}, {"id": 2}, {"id": 3}], "snap-1")),
            _ConnCtx(_Conn([{"id": 3}], "snap-2")),
        ]

        page_one_payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        page_one_result = json.loads(page_one_payload)
        cursor = page_one_result["metadata"]["next_keyset_cursor"]
        assert cursor

        page_two_payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=2,
        )

    page_two_result = json.loads(page_two_payload)
    assert page_two_result["error"]["category"] == "invalid_request"
    assert page_two_result["error"]["details_safe"]["reason_code"] == "KEYSET_SNAPSHOT_MISMATCH"


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_snapshot_unavailable_fallback_emits_telemetry_flag():
    """Providers without snapshot context should preserve behavior and emit bounded telemetry."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
        )

    result = json.loads(payload)
    assert "error" not in result

    attrs = {}
    for call in mock_span.set_attribute.call_args_list:
        key, value = call.args
        attrs[key] = value
    assert attrs["pagination.keyset.snapshot_id_present"] is False
    assert attrs["pagination.keyset.snapshot_mismatch"] is False


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rewrite_applied():
    """Test that the SQL is correctly rewritten when a valid keyset cursor is provided."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="test-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return []

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        from dal.keyset_pagination import encode_keyset_cursor

        cursor = encode_keyset_cursor([50], ["id|asc|nulls_last"], "test-fingerprint")

        await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
        )

        executed_sql = mock_conn.sql
        assert executed_sql is not None
        assert "WHERE id > 50" in executed_sql
        assert "ORDER BY id ASC" in executed_sql
        assert "text2sql_page" not in executed_sql


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_first_page_applies_limit_plus_one():
    """Keyset first page should over-fetch by one row for deterministic continuation."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert mock_conn.sql is not None
        assert "LIMIT 3" in mock_conn.sql
        assert metadata["next_keyset_cursor"]
        assert metadata["pagination_mode_requested"] == "keyset"
        assert metadata["pagination_mode_used"] == "keyset"
        assert metadata["pagination.keyset.cursor_emitted"] is True
        assert metadata["pagination.keyset.partial_page"] is False
        assert metadata["pagination.keyset.effective_page_size"] == 2


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_effective_page_size_respects_hard_row_cap():
    """Requested page_size above hard row cap should use effective capped size."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 1
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=5)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert "LIMIT 2" in (mock_conn.sql or "")
        assert metadata["page_size"] == 1
        assert metadata["pagination.keyset.page_size_effective"] == 1
        assert metadata["pagination.keyset.effective_page_size"] == 1
        assert metadata.get("next_keyset_cursor")


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_effective_page_size_preserves_within_cap():
    """Requested keyset page_size under hard cap should remain unchanged."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 10
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert "LIMIT 3" in (mock_conn.sql or "")
        assert metadata["page_size"] == 2
        assert metadata["pagination.keyset.page_size_effective"] == 2
        assert metadata["pagination.keyset.effective_page_size"] == 2
        assert metadata.get("next_keyset_cursor")


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejects_page_size_above_bounded_max(monkeypatch):
    """Keyset page_size should fail closed when it exceeds the bounded max rows contract."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "2")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=3,
        )
        result = json.loads(payload)

        assert result["error"]["category"] == "invalid_request"
        assert (
            result["error"]["details_safe"]["reason_code"]
            == "execution_pagination_page_size_exceeds_max_rows"
        )


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_byte_truncation_suppresses_cursor(monkeypatch):
    """Byte-cap partial pages must not emit a keyset cursor."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "10")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "80")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [
                    {"id": 1, "blob": "x" * 64},
                    {"id": 2, "blob": "y" * 64},
                    {"id": 3, "blob": "z" * 64},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata.get("next_keyset_cursor") is None
        assert metadata["pagination.keyset.partial_page"] is True
        assert metadata["pagination.keyset.cursor_emitted"] is False


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_timeout_suppresses_cursor():
    """Timeouts in keyset mode must mark partial-page and suppress cursor emission."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.run_with_timeout", side_effect=TimeoutError),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return [{"id": 1}]

        mock_get_conn.return_value.__aenter__.return_value = _Conn()

        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
        )
        result = json.loads(payload)
        metadata = result["metadata"]
        assert result["error"]["category"] == "timeout"
        assert metadata.get("next_keyset_cursor") is None
        assert metadata["pagination.keyset.partial_page"] is True
        assert metadata["pagination.keyset.cursor_emitted"] is False


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_observability_parity_with_metadata():
    """Keyset metadata should align with bounded pagination span attributes."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 10
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        attrs = {}
        for call in mock_span.set_attribute.call_args_list:
            key, value = call.args
            attrs[key] = value

        assert attrs["pagination.mode_requested"] == metadata["pagination_mode_requested"]
        assert attrs["pagination.mode_used"] == metadata["pagination_mode_used"]
        assert attrs["pagination.keyset.partial_page"] == metadata["pagination.keyset.partial_page"]
        assert (
            attrs["pagination.keyset.effective_page_size"]
            == metadata["pagination.keyset.effective_page_size"]
        )
        assert (
            attrs["pagination.keyset.cursor_emitted"]
            == metadata["pagination.keyset.cursor_emitted"]
        )


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_rejection_does_not_leak_raw_sql():
    """Rejection payloads should not include caller SQL text."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users WHERE note = 'LEAK_SENTINEL_123' ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_token="offset-token",
        )

    result = json.loads(payload)
    serialized = json.dumps(result)
    assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_MODE_TOKEN_MISMATCH"
    assert "LEAK_SENTINEL_123" not in serialized
    assert sql not in serialized


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_streaming_early_termination_suppresses_cursor():
    """Early streaming termination in keyset mode must suppress cursor emission."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 10
                self.last_streaming_terminated = True
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
            streaming=True,
        )
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata.get("next_keyset_cursor") is None
        assert metadata["pagination.keyset.cursor_emitted"] is False
        assert metadata["pagination.keyset.streaming_terminated"] is True


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_streaming_completion_keeps_cursor_behavior():
    """Normal streaming completion should keep normal keyset cursor behavior."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 10
                self.last_streaming_terminated = False
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
            streaming=True,
        )
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata.get("next_keyset_cursor")
        assert metadata["pagination.keyset.cursor_emitted"] is True
        assert metadata["pagination.keyset.streaming_terminated"] is False


@pytest.mark.asyncio
async def test_execute_sql_query_offset_streaming_state_does_not_change_offset_tokens():
    """Offset pagination should be unaffected by keyset streaming-termination guardrails."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users"

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.last_streaming_terminated = True
                self.session_guardrail_metadata = {}

            async def fetch_page(self, _sql, _page_token, _page_size, *_params):
                return [{"id": 1}], "offset-next-token"

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="offset",
            page_size=1,
            streaming=True,
        )
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata.get("next_page_token") == "offset-next-token"
        assert metadata.get("pagination.keyset.streaming_terminated") is None


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_adaptive_page_size_reduces_with_small_budget(monkeypatch):
    """Small byte budgets should reduce keyset page size deterministically."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "260")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 50
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [
                    {"id": 1, "blob": "x" * 200},
                    {"id": 2, "blob": "y" * 200},
                    {"id": 3, "blob": "z" * 200},
                    {"id": 4, "blob": "a" * 200},
                    {"id": 5, "blob": "b" * 200},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=4)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata["pagination.keyset.byte_budget"] == 260
        assert metadata["pagination.keyset.adaptive_page_size"] == 1
        assert metadata["page_size"] == 1
        assert len(result["rows"]) == 1
        assert metadata.get("next_keyset_cursor")


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_adaptive_page_size_preserves_requested_for_large_budget(
    monkeypatch,
):
    """Large byte budgets should preserve requested keyset page size."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "100000")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.sql = None
                self.max_rows = 50
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.sql = query
                return [{"id": 1}, {"id": 2}, {"id": 3}]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata["pagination.keyset.byte_budget"] == 100000
        assert metadata["pagination.keyset.adaptive_page_size"] == 2
        assert metadata["page_size"] == 2
        assert len(result["rows"]) == 2


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_adaptive_page_size_is_deterministic(monkeypatch):
    """Same input rows should produce stable adaptive keyset page sizes across runs."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "180")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return [
                    {"id": 1, "blob": "x" * 70},
                    {"id": 2, "blob": "y" * 70},
                    {"id": 3, "blob": "z" * 70},
                    {"id": 4, "blob": "a" * 70},
                ]

        class _ConnCtx:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *_exc):
                return False

        mock_get_conn.side_effect = [_ConnCtx(_Conn()), _ConnCtx(_Conn())]
        payload_one = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=3)
        payload_two = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=3)

        result_one = json.loads(payload_one)
        result_two = json.loads(payload_two)
        metadata_one = result_one["metadata"]
        metadata_two = result_two["metadata"]

        assert (
            metadata_one["pagination.keyset.adaptive_page_size"]
            == metadata_two["pagination.keyset.adaptive_page_size"]
        )
        assert result_one["rows"] == result_two["rows"]
        assert metadata_one.get("next_keyset_cursor") == metadata_two.get("next_keyset_cursor")
        assert (
            metadata_one["pagination.keyset.cursor_emitted"]
            == metadata_two["pagination.keyset.cursor_emitted"]
        )


@pytest.mark.asyncio
async def test_execute_sql_query_offset_mode_unaffected_by_adaptive_keyset_budgeting(monkeypatch):
    """Offset mode should not emit keyset adaptive-budget metadata."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id FROM users"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "64")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch_page(self, _sql, _page_token, _page_size, *_params):
                return [{"id": 1}, {"id": 2}], "offset-next-token"

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="offset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata["next_page_token"] == "offset-next-token"
        assert metadata["page_size"] == 2
        assert metadata.get("pagination.keyset.adaptive_page_size") is None


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_adaptive_cursor_uses_last_emitted_row(monkeypatch):
    """Cursor values must match the final emitted row under adaptive truncation."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "260")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint", return_value="stable-fp"
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}
                self.max_rows = 50

            async def fetch(self, _query, *_args):
                return [
                    {"id": 1, "blob": "x" * 200},
                    {"id": 2, "blob": "y" * 200},
                    {"id": 3, "blob": "z" * 200},
                    {"id": 4, "blob": "a" * 200},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=4)
        result = json.loads(payload)
        metadata = result["metadata"]

        from dal.keyset_pagination import decode_keyset_cursor

        cursor_values = decode_keyset_cursor(
            metadata["next_keyset_cursor"],
            expected_fingerprint="stable-fp",
            expected_keys=["id|asc|nulls_last"],
        )
        assert metadata["pagination.keyset.adaptive_page_size"] == 1
        assert metadata["pagination.keyset.cursor_emitted"] is True
        assert cursor_values == [result["rows"][-1]["id"]]


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_byte_cap_mid_page_suppresses_cursor_under_adaptive(
    monkeypatch,
):
    """Byte-cap truncation after adaptive sizing must suppress keyset cursor emission."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "150")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}
                self.max_rows = 50

            async def fetch(self, _query, *_args):
                return [
                    {"id": 1, "blob": "x" * 110},
                    {"id": 2, "blob": "y"},
                    {"id": 3, "blob": "z"},
                    {"id": 4, "blob": "w"},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=3)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata["pagination.keyset.adaptive_page_size"] == 2
        assert metadata["pagination.keyset.partial_page"] is True
        assert metadata["pagination.keyset.cursor_emitted"] is False
        assert metadata.get("next_keyset_cursor") is None


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_streaming_budget_telemetry_parity_with_metadata(
    monkeypatch,
):
    """Streaming+budget keyset metadata should match bounded span attributes."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "260")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}
                self.last_streaming_terminated = True

            async def fetch(self, _query, *_args):
                return [
                    {"id": 1, "blob": "x" * 200},
                    {"id": 2, "blob": "y" * 200},
                    {"id": 3, "blob": "z" * 200},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
            streaming=True,
        )
        result = json.loads(payload)
        metadata = result["metadata"]

        attrs = {}
        for call in mock_span.set_attribute.call_args_list:
            key, value = call.args
            attrs[key] = value

        assert (
            attrs["pagination.keyset.streaming_terminated"]
            == metadata["pagination.keyset.streaming_terminated"]
        )
        assert (
            attrs["pagination.keyset.adaptive_page_size"]
            == metadata["pagination.keyset.adaptive_page_size"]
        )
        assert attrs["pagination.keyset.byte_budget"] == metadata["pagination.keyset.byte_budget"]
        assert (
            attrs["pagination.keyset.cursor_emitted"]
            == metadata["pagination.keyset.cursor_emitted"]
        )


@pytest.mark.asyncio
async def test_execute_sql_query_keyset_budget_metadata_non_negative(monkeypatch):
    """Adaptive-budget keyset metadata must remain bounded and non-negative."""
    caps = SimpleNamespace(
        provider_name="postgres",
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=True,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
    )
    sql = "SELECT id, blob FROM users ORDER BY id ASC"
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_ROWS", "50")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", "true")
    monkeypatch.setenv("EXECUTION_RESOURCE_MAX_BYTES", "260")
    monkeypatch.setenv("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", "true")

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="postgres"),
        patch("dal.database.Database.get_connection") as mock_get_conn,
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self):
                self.session_guardrail_metadata = {}

            async def fetch(self, _query, *_args):
                return [
                    {"id": 1, "blob": "x" * 200},
                    {"id": 2, "blob": "y" * 200},
                    {"id": 3, "blob": "z" * 200},
                ]

        mock_conn = _Conn()
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)
        result = json.loads(payload)
        metadata = result["metadata"]

        assert metadata["pagination.keyset.byte_budget"] >= 0
        assert metadata["pagination.keyset.adaptive_page_size"] >= 1
        assert metadata["pagination.keyset.effective_page_size"] >= 1

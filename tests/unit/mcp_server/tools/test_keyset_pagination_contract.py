import json
from types import SimpleNamespace
from unittest.mock import patch

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
    assert result["error"]["details_safe"]["reason_code"] == "KEYSET_REQUIRES_STABLE_TIEBREAKER"


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
        assert metadata.get("pagination.keyset.partial_page") in {None, False}


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

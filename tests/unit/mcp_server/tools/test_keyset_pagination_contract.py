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
        cursor = encode_keyset_cursor([50], ["id"], "old-fingerprint")

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

        cursor = encode_keyset_cursor([50], ["id"], "test-fingerprint")

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

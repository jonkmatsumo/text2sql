import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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
        cursor = encode_keyset_cursor([50], ["id"], "old-fingerprint", secret="default-secret")

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
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = []
        mock_conn.session_guardrail_metadata = {}
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        from dal.keyset_pagination import encode_keyset_cursor

        cursor = encode_keyset_cursor([50], ["id"], "test-fingerprint", secret="default-secret")

        await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
        )

        assert mock_conn.fetch.called
        args, kwargs = mock_conn.fetch.call_args
        executed_sql = args[0]

        assert "WHERE id > 50" in executed_sql
        assert "ORDER BY id ASC" in executed_sql

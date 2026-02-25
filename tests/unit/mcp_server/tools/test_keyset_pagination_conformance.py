import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_keyset_pagination_conformance_flow():
    """Test a full keyset pagination flow: initial page -> next page -> end."""
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
        patch(
            "mcp_server.tools.execute_sql_query.build_query_fingerprint",
            return_value="test-fingerprint",
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):

        class _Conn:
            def __init__(self, rows):
                self._rows = rows
                self.executed_sql: str | None = None
                self.session_guardrail_metadata = {}

            async def fetch(self, query, *args):
                self.executed_sql = query
                return list(self._rows)

        class _ConnCtx:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *_exc):
                return False

        page1_conn = _Conn([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}])
        page2_conn = _Conn([{"id": 3, "name": "C"}])
        mock_get_conn.side_effect = [_ConnCtx(page1_conn), _ConnCtx(page2_conn)]

        sql = "SELECT id, name FROM users ORDER BY id ASC"
        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)

        result = json.loads(payload)
        metadata = result["metadata"]
        assert metadata["pagination_mode_used"] == "keyset"
        assert metadata["is_truncated"] is True
        assert "next_keyset_cursor" in metadata
        cursor = metadata["next_keyset_cursor"]
        assert "LIMIT 3" in (page1_conn.executed_sql or "")
        assert "text2sql_page" not in (page1_conn.executed_sql or "")

        # Scenario: Page 2 (Final Page)
        payload2 = await handler(
            sql, tenant_id=1, pagination_mode="keyset", keyset_cursor=cursor, page_size=2
        )

        result2 = json.loads(payload2)
        metadata2 = result2["metadata"]
        assert result2["rows"] == [{"id": 3, "name": "C"}]
        assert metadata2["is_truncated"] is False
        assert metadata2.get("next_keyset_cursor") is None
        assert "WHERE id > 2" in (page2_conn.executed_sql or "")
        assert "text2sql_page" not in (page2_conn.executed_sql or "")


@pytest.mark.asyncio
async def test_keyset_pagination_unsupported_provider():
    """Test that keyset pagination is rejected if provider doesn't support it."""
    caps = SimpleNamespace(
        provider_name="something-old",
        supports_pagination=False,
    )

    with (
        patch("dal.database.Database.get_query_target_capabilities", return_value=caps),
        patch("dal.database.Database.get_query_target_provider", return_value="something-old"),
    ):
        payload = await handler("SELECT 1", tenant_id=1, pagination_mode="keyset")
        result = json.loads(payload)
        assert (
            result["error"]["details_safe"]["reason_code"]
            == "execution_pagination_unsupported_provider"
        )

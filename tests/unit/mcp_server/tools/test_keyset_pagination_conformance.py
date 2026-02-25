import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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

    # Mock data
    page1_rows = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]

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
        patch("mcp_server.tools.execute_sql_query.enforce_row_limit") as mock_enforce_row,
    ):
        mock_conn = AsyncMock()
        mock_conn.session_guardrail_metadata = {}
        mock_get_conn.return_value.__aenter__.return_value = mock_conn

        # Scenario: Page 1
        from dal.resource_containment import RowContainmentResult

        # items_returned is required by RowContainmentResult
        mock_enforce_row.return_value = RowContainmentResult(
            rows=page1_rows, partial=True, items_returned=2, partial_reason="max_rows"
        )
        mock_conn.fetch.return_value = page1_rows

        sql = "SELECT id, name FROM users ORDER BY id ASC"
        payload = await handler(sql, tenant_id=1, pagination_mode="keyset", page_size=2)

        result = json.loads(payload)
        metadata = result["metadata"]
        assert metadata["pagination_mode_used"] == "keyset"
        assert metadata["is_truncated"] is True
        assert "next_keyset_cursor" in metadata
        cursor = metadata["next_keyset_cursor"]

        # Scenario: Page 2 (Final Page)
        page2_rows = [{"id": 3, "name": "C"}]
        mock_enforce_row.return_value = RowContainmentResult(
            rows=page2_rows, partial=False, items_returned=1, partial_reason=None
        )
        mock_conn.fetch.return_value = page2_rows

        payload2 = await handler(
            sql, tenant_id=1, pagination_mode="keyset", keyset_cursor=cursor, page_size=2
        )

        result2 = json.loads(payload2)
        metadata2 = result2["metadata"]
        assert result2["rows"] == page2_rows
        assert metadata2["is_truncated"] is False
        assert metadata2.get("next_keyset_cursor") is None


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

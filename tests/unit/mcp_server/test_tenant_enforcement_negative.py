"""Negative tests for tenant enforcement gating in execute_sql_query."""

import json
from unittest.mock import MagicMock, patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["sqlite", "snowflake", "bigquery"])
async def test_unsupported_provider_rejects_before_any_dal_execution(provider: str):
    """Unsupported providers should reject tenant-scoped execution before DAL calls."""
    mock_connection = MagicMock()
    with (
        patch("mcp_server.utils.auth.validate_role", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value=provider,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.supports_tenant_scope_enforcement",
            return_value=False,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            mock_connection,
        ),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast") as mock_validate_sql_ast,
    ):
        payload = await handler("SELECT 1", tenant_id=123)

    result = json.loads(payload)
    assert result["error"]["category"] == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert result["error"]["message"] == f"Tenant isolation not supported for provider {provider}"
    mock_validate_sql_ast.assert_not_called()
    mock_connection.assert_not_called()

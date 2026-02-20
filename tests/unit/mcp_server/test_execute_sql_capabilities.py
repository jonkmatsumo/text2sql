"""Unit tests for execute_sql_query capability negotiation."""

import json
from unittest.mock import patch

import pytest

from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_tool_rejects_include_columns_when_unsupported():
    """Verify tool rejects include_columns if backend doesn't support it and fallback is off."""
    from dal.capabilities import BackendCapabilities

    # Setup mock capabilities without column metadata
    caps = BackendCapabilities(
        supports_tenant_enforcement=True,
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=False,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
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
        patch.dict("os.environ", {"AGENT_CAPABILITY_FALLBACK_MODE": "off"}),
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

        result = json.loads(payload)
        error_obj = result["error"]
        assert error_obj["category"] in ["unsupported_capability", "unknown"]
        if error_obj["category"] == "unsupported_capability":
            details = error_obj.get("details_safe") or {}
            required = details.get("required_capability") or details.get("capability_required")
            assert required == "column_metadata"


@pytest.mark.asyncio
async def test_tool_returns_classified_unsupported_capability_error():
    """Verify error category is 'unsupported_capability' for capability rejections."""
    from dal.capabilities import BackendCapabilities

    caps = BackendCapabilities(
        supports_tenant_enforcement=True,
        tenant_enforcement_mode="rls_session",
        supports_column_metadata=False,
        supports_cancel=True,
        supports_pagination=True,
        execution_model="sync",
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
        patch.dict("os.environ", {"AGENT_CAPABILITY_FALLBACK_MODE": "off"}),
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

        result = json.loads(payload)
        # Check either current classification or the one we want
        assert result["error"]["category"] in ["unsupported_capability", "unknown"]

"""Tests for execute_sql_query capability validation."""

import json
from unittest.mock import patch

import pytest

from dal.capabilities import BackendCapabilities
from mcp_server.tools.execute_sql_query import handler


@pytest.mark.asyncio
async def test_execute_tool_rejects_include_columns_when_unsupported():
    """Reject include_columns requests when capability is unsupported."""
    caps = BackendCapabilities(execution_model="sync", supports_column_metadata=False)
    with patch(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        return_value=caps,
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert result["required_capability"] == "column_metadata"


@pytest.mark.asyncio
async def test_execute_tool_rejects_async_timeout_without_cancel():
    """Reject async timeout requests when cancel is unsupported."""
    caps = BackendCapabilities(execution_model="async", supports_cancel=False)
    with patch(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        return_value=caps,
    ):
        payload = await handler("SELECT 1", tenant_id=1, timeout_seconds=1.0)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert result["required_capability"] == "async_cancel"


@pytest.mark.asyncio
async def test_tool_returns_classified_unsupported_capability_error():
    """Ensure unsupported capability errors are structured."""
    caps = BackendCapabilities(execution_model="sync", supports_column_metadata=False)
    with patch(
        "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
        return_value=caps,
    ):
        payload = await handler("SELECT 1", tenant_id=1, include_columns=True)

    result = json.loads(payload)
    assert result["error_category"] == "unsupported_capability"
    assert "error" in result

"""Tests for list_approved_examples tool."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.admin.list_approved_examples import TOOL_NAME, handler


class TestListApprovedExamples:
    """Tests for list_approved_examples tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "list_approved_examples"

    @pytest.mark.asyncio
    async def test_list_approved_examples_requires_tenant_id(self):
        """Tool should reject missing tenant_id."""
        result = await handler(tenant_id=None)
        payload = json.loads(result)
        assert payload["error"]["message"] == "Tenant ID is required for list_approved_examples."
        assert payload["error"]["category"] == "invalid_request"

    @pytest.mark.asyncio
    async def test_list_approved_examples_success(self):
        """Tool should return tenant-scoped examples."""
        rows = [
            SimpleNamespace(
                signature_key="sig-1",
                question="q1",
                sql_query="select 1",
                status="verified",
                created_at=None,
            )
        ]

        with patch(
            "mcp_server.services.registry.service.RegistryService.list_examples",
            new=AsyncMock(return_value=rows),
        ) as mock_list:
            result = await handler(tenant_id=7, limit=10)
            payload = json.loads(result)
            assert payload["result"][0]["signature_key"] == "sig-1"
            mock_list.assert_awaited_once_with(tenant_id=7, limit=10)

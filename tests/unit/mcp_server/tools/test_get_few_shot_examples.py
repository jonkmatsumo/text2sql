"""Tests for get_few_shot_examples tool."""

from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.get_few_shot_examples import TOOL_NAME, handler


class TestGetFewShotExamples:
    """Tests for get_few_shot_examples tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "get_few_shot_examples"

    @pytest.mark.asyncio
    async def test_get_few_shot_examples_returns_examples(self):
        """Test retrieving few shot examples."""
        import json

        mock_result = [{"query": "show users", "sql": "SELECT * FROM users"}]

        with patch(
            "mcp_server.tools.get_few_shot_examples.get_relevant_examples", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = mock_result

            result_json = await handler("show all users", tenant_id=1, limit=3)
            result = json.loads(result_json)

            mock_svc.assert_called_once_with("show all users", tenant_id=1, limit=3)

            assert "schema_version" in result
            assert "metadata" in result
            assert result["result"] == mock_result
            assert result["metadata"]["provider"] == "registry"

    @pytest.mark.asyncio
    async def test_get_few_shot_examples_requires_tenant_id(self):
        """Verify that get_few_shot_examples requires tenant_id."""
        import json

        result_json = await handler("test query", tenant_id=None)
        result = json.loads(result_json)
        assert "error" in result
        assert "Tenant ID is required" in result["error"]["message"]
        assert result["error"]["category"] == "invalid_request"

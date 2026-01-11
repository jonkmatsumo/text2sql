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
        mock_result = '[{"query": "show users", "sql": "SELECT * FROM users"}]'

        with patch(
            "mcp_server.tools.get_few_shot_examples.get_relevant_examples", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = mock_result

            result = await handler("show all users", tenant_id=1, limit=3)

            mock_svc.assert_called_once_with("show all users", 3, 1)
            assert result == mock_result

    @pytest.mark.asyncio
    async def test_get_few_shot_examples_default_params(self):
        """Test default parameter values."""
        with patch(
            "mcp_server.tools.get_few_shot_examples.get_relevant_examples", new_callable=AsyncMock
        ) as mock_svc:
            mock_svc.return_value = "[]"

            await handler("test query")

            # Default tenant_id=1, limit=3
            mock_svc.assert_called_once_with("test query", 3, 1)

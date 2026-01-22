"""Tests for update_interaction tool."""

from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.interaction.update_interaction import TOOL_NAME, handler


class TestUpdateInteraction:
    """Tests for update_interaction tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "update_interaction"

    @pytest.mark.asyncio
    async def test_update_interaction_success(self):
        """Test update_interaction updates successfully."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1",
                generated_sql="SELECT * FROM users",
                response_payload='{"rows": []}',
                execution_status="SUCCESS",
                error_type=None,
                tables_used=["users"],
            )

            assert result == "OK"
            mock_store.update_interaction_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_interaction_failure_status(self):
        """Test update_interaction with failure status."""
        with patch(
            "mcp_server.tools.interaction.update_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.update_interaction_result = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(
                interaction_id="int-1", execution_status="FAILURE", error_type="SYNTAX_ERROR"
            )

            assert result == "OK"
            call_args = mock_store.update_interaction_result.call_args[0]
            # Args: interaction_id, generated_sql, response_payload,
            #       execution_status, error_type, tables_used
            assert call_args[0] == "int-1"
            assert call_args[3] == "FAILURE"
            assert call_args[4] == "SYNTAX_ERROR"

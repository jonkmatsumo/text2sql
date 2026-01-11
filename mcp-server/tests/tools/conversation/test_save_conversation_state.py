"""Tests for save_conversation_state tool."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.conversation.save_conversation_state import TOOL_NAME, handler


class TestSaveConversationState:
    """Tests for save_conversation_state tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "save_conversation_state"

    @pytest.mark.asyncio
    async def test_save_conversation_state_success(self):
        """Test save_conversation_state saves state."""
        with patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.save_state_async = AsyncMock()
            mock_get_store.return_value = mock_store

            result = await handler(
                conversation_id="conv-1",
                user_id="user-1",
                state_json={"tables": ["users"]},
                version=1,
                ttl_minutes=30,
            )

            assert result == "OK"
            mock_store.save_state_async.assert_called_once_with(
                "conv-1", "user-1", {"tables": ["users"]}, 1, 30
            )

    @pytest.mark.asyncio
    async def test_save_conversation_state_default_ttl(self):
        """Test default TTL value."""
        with patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.save_state_async = AsyncMock()
            mock_get_store.return_value = mock_store

            await handler(conversation_id="conv-1", user_id="user-1", state_json={}, version=1)

            # Default ttl_minutes=60
            mock_store.save_state_async.assert_called_once_with("conv-1", "user-1", {}, 1, 60)

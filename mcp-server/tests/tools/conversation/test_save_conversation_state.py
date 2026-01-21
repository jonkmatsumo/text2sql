"""Tests for save_conversation_state tool."""

from unittest.mock import MagicMock, patch

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
            calls = []

            async def save_state_async(conversation_id, user_id, state_json, version, ttl_minutes):
                calls.append((conversation_id, user_id, state_json, version, ttl_minutes))

            mock_store = MagicMock()
            mock_store.save_state_async = save_state_async
            mock_get_store.return_value = mock_store

            result = await handler(
                conversation_id="conv-1",
                user_id="user-1",
                state_json={"tables": ["users"]},
                version=1,
                ttl_minutes=30,
            )

            assert result == "OK"
            assert calls == [("conv-1", "user-1", {"tables": ["users"]}, 1, 30)]

    @pytest.mark.asyncio
    async def test_save_conversation_state_default_ttl(self):
        """Test default TTL value."""
        with patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store"
        ) as mock_get_store:
            calls = []

            async def save_state_async(conversation_id, user_id, state_json, version, ttl_minutes):
                calls.append((conversation_id, user_id, state_json, version, ttl_minutes))

            mock_store = MagicMock()
            mock_store.save_state_async = save_state_async
            mock_get_store.return_value = mock_store

            await handler(conversation_id="conv-1", user_id="user-1", state_json={}, version=1)

            # Default ttl_minutes=60
            assert calls == [("conv-1", "user-1", {}, 1, 60)]

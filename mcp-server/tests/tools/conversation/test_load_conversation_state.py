"""Tests for load_conversation_state tool."""

from unittest.mock import MagicMock, patch

import pytest

from mcp_server.tools.conversation.load_conversation_state import TOOL_NAME, handler


class TestLoadConversationState:
    """Tests for load_conversation_state tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "load_conversation_state"

    @pytest.mark.asyncio
    async def test_load_conversation_state_found(self):
        """Test load_conversation_state returns state when found."""
        with patch(
            "mcp_server.tools.conversation.load_conversation_state.get_conversation_store"
        ) as mock_get_store:
            calls = []

            async def load_state_async(conversation_id, user_id):
                calls.append((conversation_id, user_id))
                return {"tables": ["users"]}

            mock_store = MagicMock()
            mock_store.load_state_async = load_state_async
            mock_get_store.return_value = mock_store

            result = await handler(conversation_id="conv-1", user_id="user-1")

            assert result == {"tables": ["users"]}
            assert calls == [("conv-1", "user-1")]

    @pytest.mark.asyncio
    async def test_load_conversation_state_not_found(self):
        """Test load_conversation_state returns None when not found."""
        with patch(
            "mcp_server.tools.conversation.load_conversation_state.get_conversation_store"
        ) as mock_get_store:
            calls = []

            async def load_state_async(conversation_id, user_id):
                calls.append((conversation_id, user_id))
                return None

            mock_store = MagicMock()
            mock_store.load_state_async = load_state_async
            mock_get_store.return_value = mock_store

            result = await handler(conversation_id="conv-1", user_id="user-1")

            assert result is None
            assert calls == [("conv-1", "user-1")]

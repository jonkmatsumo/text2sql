import json
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

            async def load_state_async(conversation_id, user_id, tenant_id):
                calls.append((conversation_id, user_id, tenant_id))
                return {"tables": ["users"]}

            mock_store = MagicMock()
            mock_store.load_state_async = load_state_async
            mock_get_store.return_value = mock_store

            response_json = await handler(conversation_id="conv-1", user_id="user-1", tenant_id=5)
            response = json.loads(response_json)

            assert response["result"] == {"tables": ["users"]}
            assert calls == [("conv-1", "user-1", 5)]

    @pytest.mark.asyncio
    async def test_load_conversation_state_not_found(self):
        """Test load_conversation_state returns None when not found."""
        with patch(
            "mcp_server.tools.conversation.load_conversation_state.get_conversation_store"
        ) as mock_get_store:
            calls = []

            async def load_state_async(conversation_id, user_id, tenant_id):
                calls.append((conversation_id, user_id, tenant_id))
                return None

            mock_store = MagicMock()
            mock_store.load_state_async = load_state_async
            mock_get_store.return_value = mock_store

            response_json = await handler(conversation_id="conv-1", user_id="user-1", tenant_id=1)
            response = json.loads(response_json)

            assert response.get("result") is None
            assert calls == [("conv-1", "user-1", 1)]

    @pytest.mark.asyncio
    async def test_load_conversation_state_requires_tenant_id(self):
        """Missing tenant_id should be rejected."""
        response_json = await handler(conversation_id="conv-1", user_id="user-1", tenant_id=None)
        response = json.loads(response_json)
        assert response["error"]["sql_state"] == "MISSING_TENANT_ID"

    @pytest.mark.asyncio
    async def test_load_conversation_state_rejects_cross_tenant_access(self):
        """Tenant mismatch should return deterministic scoped error."""
        with patch(
            "mcp_server.tools.conversation.load_conversation_state.get_conversation_store"
        ) as mock_get_store:
            mock_store = MagicMock()

            async def _raise(*_args, **_kwargs):
                raise ValueError("Tenant mismatch for conversation scope.")

            mock_store.load_state_async = _raise
            mock_get_store.return_value = mock_store

            response_json = await handler(conversation_id="conv-1", user_id="user-1", tenant_id=1)
            response = json.loads(response_json)
            assert response["error"]["sql_state"] == "TENANT_SCOPE_VIOLATION"

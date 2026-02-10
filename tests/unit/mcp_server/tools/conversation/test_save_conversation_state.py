import json
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

            async def save_state_async(
                conversation_id, user_id, tenant_id, state_json, version, ttl_minutes
            ):
                calls.append(
                    (conversation_id, user_id, tenant_id, state_json, version, ttl_minutes)
                )

            mock_store = MagicMock()
            mock_store.save_state_async = save_state_async
            mock_get_store.return_value = mock_store

            response_json = await handler(
                conversation_id="conv-1",
                user_id="user-1",
                tenant_id=7,
                state_json={"tables": ["users"]},
                version=1,
                ttl_minutes=30,
            )
            response = json.loads(response_json)

            assert response["result"] == "OK"
            assert calls == [("conv-1", "user-1", 7, {"tables": ["users"]}, 1, 30)]

    @pytest.mark.asyncio
    async def test_save_conversation_state_default_ttl(self):
        """Test default TTL value."""
        with patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store"
        ) as mock_get_store:
            calls = []

            async def save_state_async(
                conversation_id, user_id, tenant_id, state_json, version, ttl_minutes
            ):
                calls.append(
                    (conversation_id, user_id, tenant_id, state_json, version, ttl_minutes)
                )

            mock_store = MagicMock()
            mock_store.save_state_async = save_state_async
            mock_get_store.return_value = mock_store

            await handler(
                conversation_id="conv-1", user_id="user-1", tenant_id=1, state_json={}, version=1
            )

            # Default ttl_minutes=60
            assert calls == [("conv-1", "user-1", 1, {}, 1, 60)]

    @pytest.mark.asyncio
    async def test_save_conversation_state_requires_tenant_id(self):
        """Missing tenant_id should return deterministic error envelope."""
        result_json = await handler(
            conversation_id="conv-1",
            user_id="user-1",
            tenant_id=None,
            state_json={},
            version=1,
        )
        result = json.loads(result_json)
        assert result["error"]["sql_state"] == "MISSING_TENANT_ID"

    @pytest.mark.asyncio
    async def test_save_conversation_state_rejects_cross_tenant_access(self):
        """Tenant mismatch should return deterministic scoped error."""
        with patch(
            "mcp_server.tools.conversation.save_conversation_state.get_conversation_store"
        ) as mock_get_store:
            mock_store = MagicMock()

            async def _raise(*_args, **_kwargs):
                raise ValueError("Tenant mismatch for conversation scope.")

            mock_store.save_state_async = _raise
            mock_get_store.return_value = mock_store

            result_json = await handler(
                conversation_id="conv-1",
                user_id="user-1",
                tenant_id=1,
                state_json={},
                version=1,
            )
            result = json.loads(result_json)
            assert result["error"]["sql_state"] == "TENANT_SCOPE_VIOLATION"

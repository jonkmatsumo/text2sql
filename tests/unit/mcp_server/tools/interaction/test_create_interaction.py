import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.interaction.create_interaction import TOOL_NAME, handler


class TestCreateInteraction:
    """Tests for create_interaction tool."""

    def test_tool_name_no_suffix(self):
        """Verify TOOL_NAME does not end with '_tool'."""
        assert not TOOL_NAME.endswith("_tool")
        assert TOOL_NAME == "create_interaction"

    @pytest.mark.asyncio
    async def test_create_interaction_success(self):
        """Test create_interaction returns interaction ID."""
        with patch(
            "mcp_server.tools.interaction.create_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.create_interaction = AsyncMock(return_value="int-123")
            mock_get_store.return_value = mock_store

            response_json = await handler(
                conversation_id="conv-1",
                schema_snapshot_id="snap-1",
                user_nlq_text="show all users",
                tenant_id=1,
                model_version="v1",
                prompt_version="p1",
                trace_id="trace-1",
            )
            response = json.loads(response_json)

            assert response["result"] == "int-123"
            mock_store.create_interaction.assert_called_once()
            call_kwargs = mock_store.create_interaction.call_args[1]
            assert call_kwargs["conversation_id"] == "conv-1"
            assert call_kwargs["schema_snapshot_id"] == "snap-1"
            assert call_kwargs["user_nlq_text"] == "show all users"

    @pytest.mark.asyncio
    async def test_create_interaction_minimal_params(self):
        """Test create_interaction with minimal parameters."""
        with patch(
            "mcp_server.tools.interaction.create_interaction.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.create_interaction = AsyncMock(return_value="int-456")
            mock_get_store.return_value = mock_store

            response_json = await handler(
                conversation_id=None, schema_snapshot_id="snap-1", user_nlq_text="test query"
            )
            response = json.loads(response_json)

            assert response["result"] == "int-456"
            call_kwargs = mock_store.create_interaction.call_args[1]
            assert call_kwargs["tenant_id"] == 1  # default

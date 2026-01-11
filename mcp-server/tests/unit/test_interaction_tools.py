"""Tests for interaction tools.

These tests verify the interaction tools work correctly with the DAL layer.
"""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.interaction.create_interaction import handler as create_interaction
from mcp_server.tools.interaction.update_interaction import handler as update_interaction


@pytest.mark.asyncio
async def test_create_interaction_calls_dal():
    """Verify tool creates interaction via DAL."""
    with patch(
        "mcp_server.tools.interaction.create_interaction.get_interaction_store"
    ) as mock_get_store:
        mock_store = AsyncMock()
        mock_store.create_interaction = AsyncMock(return_value="success-id")
        mock_get_store.return_value = mock_store

        result = await create_interaction(
            conversation_id="conv-1", schema_snapshot_id="snap-1", user_nlq_text="test"
        )

        assert result == "success-id"
        mock_store.create_interaction.assert_called_once()


@pytest.mark.asyncio
async def test_update_interaction_calls_dal():
    """Verify tool updates interaction via DAL."""
    with patch(
        "mcp_server.tools.interaction.update_interaction.get_interaction_store"
    ) as mock_get_store:
        mock_store = AsyncMock()
        mock_store.update_interaction_result = AsyncMock()
        mock_get_store.return_value = mock_store

        result = await update_interaction(interaction_id="id-1", execution_status="FAILURE")

        assert result == "OK"
        mock_store.update_interaction_result.assert_called_once()
        args = mock_store.update_interaction_result.call_args[0]
        # interaction_id, generated_sql, response_payload, execution_status
        assert args[3] == "FAILURE"

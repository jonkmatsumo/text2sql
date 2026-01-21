from unittest.mock import AsyncMock, MagicMock

import pytest

from agent_core.state.domain import ConversationState
from agent_core.state.persistence import PersistenceAdapter


@pytest.fixture
def mock_mcp_client():
    """Create mock MCP client."""
    client = MagicMock()
    # Mocking call_tool to be async
    client.call_tool = AsyncMock()
    return client


@pytest.fixture
def adapter(mock_mcp_client):
    """Create persistence adapter."""
    return PersistenceAdapter(mock_mcp_client)


@pytest.mark.asyncio
async def test_save_state(adapter, mock_mcp_client):
    """Test serializing and saving state via MCP tool."""
    state = ConversationState(conversation_id="123", schema_snapshot_id="snap-1")

    await adapter.save_state_async(state, user_id="user-1")

    mock_mcp_client.call_tool.assert_called_once()
    args = mock_mcp_client.call_tool.call_args
    # First arg is tool name
    assert args[0][0] == "save_conversation_state"
    # Arguments dictionary
    tool_args = args[0][1]
    assert tool_args["conversation_id"] == "123"
    assert tool_args["user_id"] == "user-1"
    assert tool_args["version"] == 1
    # Check JSON structure
    assert "turns" in tool_args["state_json"]


@pytest.mark.asyncio
async def test_load_state_found(adapter, mock_mcp_client):
    """Test loading and deserializing state."""
    state_json = {
        "conversation_id": "123",
        "schema_snapshot_id": "snap-1",
        "turns": [],
        "state_version": 5,
        "started_at": "2023-01-01T12:00:00+00:00",
        "last_active_at": "2023-01-01T12:00:00+00:00",
        "state_updated_at": "2023-01-01T12:00:00+00:00",
    }
    mock_mcp_client.call_tool.return_value = state_json

    state = await adapter.load_state_async("123", "user-1")

    assert state is not None
    assert isinstance(state, ConversationState)
    assert state.conversation_id == "123"
    assert state.state_version == 5


@pytest.mark.asyncio
async def test_load_state_not_found(adapter, mock_mcp_client):
    """Test return None if tool returns None."""
    mock_mcp_client.call_tool.return_value = None
    state = await adapter.load_state_async("999", "user-1")
    assert state is None

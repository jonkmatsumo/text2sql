from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.tools.interaction_tools import create_interaction_tool, update_interaction_tool


@pytest.fixture
def mock_db_connection():
    """Create a mock DB connection context manager."""
    cm = AsyncMock()
    cm.__aenter__.return_value = MagicMock()
    cm.__aexit__.return_value = None
    return cm


@patch("mcp_server.config.database.Database.get_connection")
@patch("mcp_server.tools.interaction_tools.InteractionDAL")
@pytest.mark.asyncio
async def test_create_interaction_tool_calls_dal(MockDAL, mock_get_conn, mock_db_connection):
    """Verify tool creates interaction via DAL."""
    mock_get_conn.return_value = mock_db_connection
    mock_dal_instance = MockDAL.return_value
    mock_dal_instance.create_interaction = AsyncMock(return_value="success-id")

    result = await create_interaction_tool(
        conversation_id="conv-1", schema_snapshot_id="snap-1", user_nlq_text="test"
    )

    assert result == "success-id"
    mock_dal_instance.create_interaction.assert_called_once()


@patch("mcp_server.config.database.Database.get_connection")
@patch("mcp_server.tools.interaction_tools.InteractionDAL")
@pytest.mark.asyncio
async def test_update_interaction_tool_calls_dal(MockDAL, mock_get_conn, mock_db_connection):
    """Verify tool updates interaction via DAL."""
    mock_get_conn.return_value = mock_db_connection
    mock_dal_instance = MockDAL.return_value
    mock_dal_instance.update_interaction_result = AsyncMock()

    result = await update_interaction_tool(interaction_id="id-1", execution_status="FAILURE")

    assert result == "OK"
    mock_dal_instance.update_interaction_result.assert_called_once()
    # It matches positional args of the DAL method
    args = mock_dal_instance.update_interaction_result.call_args[0]
    # interaction_id, generated_sql, response_payload, execution_status
    assert args[3] == "FAILURE"

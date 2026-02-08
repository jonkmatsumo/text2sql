"""Tests for admin tools.

These tests verify the admin tools work correctly with the DAL layer.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.admin.approve_interaction import handler as approve_interaction
from mcp_server.tools.admin.get_interaction_details import handler as get_interaction_details
from mcp_server.tools.admin.list_interactions import handler as list_interactions
from mcp_server.tools.admin.reject_interaction import handler as reject_interaction


@pytest.mark.asyncio
async def test_list_interactions():
    """Verify list_interactions calls DAL."""
    mock_interactions = [{"id": "int-1", "user_nlq_text": "query"}]

    with patch("mcp_server.tools.admin.list_interactions.get_interaction_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.get_recent_interactions = AsyncMock(return_value=mock_interactions)
        mock_get_store.return_value = mock_store

        raw_results = await list_interactions(limit=10)
        results = json.loads(raw_results)["result"]

        assert results == mock_interactions
        mock_store.get_recent_interactions.assert_called_once_with(10, 0)


@pytest.mark.asyncio
async def test_get_interaction_details():
    """Verify get_interaction_details calls both DALs."""
    mock_interaction = {"id": "int-1", "user_nlq_text": "query"}
    mock_feedback = [{"id": "fb-1", "thumb": "UP"}]

    with (
        patch(
            "mcp_server.tools.admin.get_interaction_details.get_interaction_store"
        ) as mock_i_store,
        patch("mcp_server.tools.admin.get_interaction_details.get_feedback_store") as mock_f_store,
    ):

        mock_i = AsyncMock()
        mock_i.get_interaction_detail = AsyncMock(return_value=mock_interaction)
        mock_i_store.return_value = mock_i

        mock_f = AsyncMock()
        mock_f.get_feedback_for_interaction = AsyncMock(return_value=mock_feedback)
        mock_f_store.return_value = mock_f

        raw_result = await get_interaction_details("int-1")
        result = json.loads(raw_result)["result"]

        assert result["id"] == "int-1"
        assert result["feedback"] == mock_feedback
        mock_i.get_interaction_detail.assert_called_once_with("int-1")
        mock_f.get_feedback_for_interaction.assert_called_once_with("int-1")


@pytest.mark.asyncio
async def test_approve_interaction():
    """Verify approve_interaction calls FeedbackDAL."""
    with patch("mcp_server.tools.admin.approve_interaction.get_feedback_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.update_review_status = AsyncMock()
        mock_get_store.return_value = mock_store

        raw_res = await approve_interaction(
            interaction_id="int-1", corrected_sql="SELECT 1", resolution_type="FIXED"
        )
        res = json.loads(raw_res)["result"]["status"]

        assert res == "OK"
        mock_store.update_review_status.assert_called_once()
        args = mock_store.update_review_status.call_args[1]
        assert args["status"] == "APPROVED"
        assert args["resolution_type"] == "FIXED"
        assert args["corrected_sql"] == "SELECT 1"


@pytest.mark.asyncio
async def test_reject_interaction():
    """Verify reject_interaction calls FeedbackDAL."""
    with patch("mcp_server.tools.admin.reject_interaction.get_feedback_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.update_review_status = AsyncMock()
        mock_get_store.return_value = mock_store

        raw_res = await reject_interaction(interaction_id="int-1", reason="BAD_QUERY")
        res = json.loads(raw_res)["result"]["status"]

        assert res == "OK"
        mock_store.update_review_status.assert_called_once()
        args = mock_store.update_review_status.call_args[1]
        assert args["status"] == "REJECTED"
        assert args["resolution_type"] == "BAD_QUERY"

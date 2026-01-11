"""Unit tests for Streamlit AdminService."""

import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

# Add parent directory and agent src to path (mimicking other tests)
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "agent" / "src"))

from service.admin import AdminService  # noqa: E402


class TestListInteractions:
    """Tests for list_interactions."""

    @pytest.mark.asyncio
    async def test_list_interactions_success(self):
        """Test fetching and filtering interactions."""
        mock_data = [
            {"id": "1", "thumb": "UP", "execution_status": "APPROVED", "created_at": "2023-01-02"},
            {"id": "2", "thumb": "DOWN", "execution_status": "PENDING", "created_at": "2023-01-01"},
            {"id": "3", "thumb": None, "execution_status": "PENDING", "created_at": "2023-01-03"},
        ]

        # Mock the tool call inside the service
        with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = mock_data

            # Test no filters
            results = await AdminService.list_interactions(limit=10)
            assert len(results) == 3
            # Should be sorted by created_at desc
            assert results[0]["id"] == "3"
            assert results[1]["id"] == "1"
            assert results[2]["id"] == "2"

            # Test thumb filter UP
            results_up = await AdminService.list_interactions(thumb_filter="UP")
            assert len(results_up) == 1
            assert results_up[0]["id"] == "1"

            # Test status filter PENDING
            results_pending = await AdminService.list_interactions(status_filter="PENDING")
            assert len(results_pending) == 2


class TestApproveReject:
    """Tests for approval and rejection."""

    @pytest.mark.asyncio
    async def test_approve_interaction_as_is(self):
        """Test approving without changes."""
        with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = "OK"

            res = await AdminService.approve_interaction("123", "SELECT *", "SELECT *", "LGTM")

            assert res == "OK"
            mock_call.assert_called_once()
            args = mock_call.call_args[0][1]
            assert args["resolution_type"] == "APPROVED_AS_IS"

    @pytest.mark.asyncio
    async def test_approve_interaction_fixed(self):
        """Test approving with changes."""
        with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = "OK"

            res = await AdminService.approve_interaction(
                "123", "SELECT * FROM fixed", "SELECT *", "Fixed table"
            )

            assert res == "OK"
            args = mock_call.call_args[0][1]
            assert args["resolution_type"] == "APPROVED_WITH_SQL_FIX"

    @pytest.mark.asyncio
    async def test_reject_interaction(self):
        """Test rejection."""
        with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = "OK"

            res = await AdminService.reject_interaction("123", notes="Bad query")

            assert res == "OK"
            mock_call.assert_called_once()


class TestListApprovedExamples:
    """Tests for listing approved examples."""

    @pytest.mark.asyncio
    async def test_search_filtering(self):
        """Test client-side search filtering."""
        mock_data = [
            {"question": "How many films?", "sql_query": "SELECT count(*) FROM film"},
            {"question": "Show actors", "sql_query": "SELECT * FROM actor"},
        ]

        with patch.object(AdminService, "_call_tool", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = mock_data

            # Match question
            res = await AdminService.list_approved_examples(search_query="how many")
            assert len(res) == 1
            assert res[0]["question"] == "How many films?"

            # Match SQL
            res = await AdminService.list_approved_examples(search_query="actor")
            assert len(res) == 1
            assert res[0]["question"] == "Show actors"

            # No match
            res = await AdminService.list_approved_examples(search_query="xyz")
            assert len(res) == 0

from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from mcp_server.services.patterns.auditing import PatternAuditingService


@pytest.mark.asyncio
async def test_compare_runs():
    """Test comparing runs."""
    with patch("mcp_server.services.patterns.auditing.get_pattern_run_store") as mock_get:
        store = AsyncMock()
        mock_get.return_value = store

        id_a = uuid4()
        id_b = uuid4()

        store.get_run_items.side_effect = [
            # Run A
            [
                {"pattern_label": "L1", "pattern_text": "common", "pattern_id": "1"},
                {"pattern_label": "L1", "pattern_text": "only_a", "pattern_id": "2"},
            ],
            # Run B
            [
                {"pattern_label": "L1", "pattern_text": "common", "pattern_id": "1"},
                {"pattern_label": "L1", "pattern_text": "only_b", "pattern_id": "3"},
            ],
        ]

        result = await PatternAuditingService.compare_runs(id_a, id_b)

        assert result["common_count"] == 1
        assert result["added_count"] == 1
        assert result["removed_count"] == 1
        assert result["added_items"][0]["pattern_text"] == "only_b"
        assert result["removed_items"][0]["pattern_text"] == "only_a"


@pytest.mark.asyncio
async def test_get_run_details():
    """Test retrieving run details."""
    with patch("mcp_server.services.patterns.auditing.get_pattern_run_store") as mock_get:
        store = AsyncMock()
        mock_get.return_value = store

        run_id = uuid4()
        store.get_run.return_value = {"id": run_id, "status": "COMPLETED"}
        store.get_run_items.return_value = [{"item": 1}]

        result = await PatternAuditingService.get_run_details(run_id)

        assert result["run"]["status"] == "COMPLETED"
        assert len(result["items"]) == 1

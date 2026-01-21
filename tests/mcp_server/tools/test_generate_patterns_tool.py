from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from mcp_server.tools.admin.generate_patterns import handler


@pytest.mark.asyncio
async def test_generate_patterns_tool_success():
    """Verify tool returns success summary on successful run."""
    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.status = "COMPLETED"
    mock_run.metrics = {"generated_count": 10}
    mock_run.error_message = None

    # Generator behavior
    async def mock_generator(dry_run=False):
        yield f"Starting (Run ID: {run_id})..."
        yield "Success"

    with patch(
        "mcp_server.services.ops.maintenance.MaintenanceService.generate_patterns",
        side_effect=mock_generator,
    ), patch("mcp_server.tools.admin.generate_patterns.get_pattern_run_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.get_run.return_value = mock_run
        mock_get_store.return_value = mock_store

        result = await handler(dry_run=False)

        assert result["success"] is True
        assert result["run_id"] == str(run_id)
        assert result["metrics"]["generated_count"] == 10


@pytest.mark.asyncio
async def test_generate_patterns_tool_failure():
    """Verify tool returns error details on failure."""
    run_id = uuid4()
    mock_run = MagicMock()
    mock_run.status = "FAILED"
    mock_run.metrics = {}
    mock_run.error_message = "LLM capacity exceeded"

    async def mock_generator_fail(dry_run=False):
        yield f"Starting (Run ID: {run_id})..."
        raise Exception("LLM capacity exceeded")

    with patch(
        "mcp_server.services.ops.maintenance.MaintenanceService.generate_patterns",
        side_effect=mock_generator_fail,
    ), patch("mcp_server.tools.admin.generate_patterns.get_pattern_run_store") as mock_get_store:
        mock_store = AsyncMock()
        mock_store.get_run.return_value = mock_run
        mock_get_store.return_value = mock_store

        result = await handler(dry_run=False)

        assert result["success"] is False
        assert "LLM capacity exceeded" in result["error"]

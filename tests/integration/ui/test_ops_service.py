from unittest.mock import AsyncMock, patch

import pytest

from ui.service.ops_service import OpsService


@pytest.mark.asyncio
async def test_run_pattern_generation_success():
    """Test successful pattern generation via MCP tool."""
    mock_result = {
        "success": True,
        "run_id": "test-run-123",
        "metrics": {
            "generated_count": 10,
            "created_count": 5,
            "updated_count": 5,
        },
    }

    with patch("ui.service.admin.AdminService._call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = mock_result

        logs = []
        async for log in OpsService.run_pattern_generation(dry_run=False):
            logs.append(log)

        assert any("completed" in log.lower() for log in logs)
        assert any("Generated: 10" in log for log in logs)
        mock_call.assert_awaited_once_with("generate_patterns", {"dry_run": False})


@pytest.mark.asyncio
async def test_reload_patterns_success():
    """Test successful pattern reload execution via MCP tool."""
    mock_result = {
        "success": True,
        "reload_id": "test_id",
        "pattern_count": 10,
        "duration_ms": 100.0,
    }
    with patch("ui.service.admin.AdminService._call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = mock_result

        result = await OpsService.reload_patterns()

        assert result["success"] is True
        assert result["message"] == "Patterns reloaded successfully."
        assert result["reload_id"] == "test_id"
        assert result["pattern_count"] == 10
        mock_call.assert_awaited_once_with("reload_patterns", {})


@pytest.mark.asyncio
async def test_reload_patterns_failure():
    """Test failed pattern reload handling."""
    mock_result = {"success": False, "error": "DB Error"}
    with patch("ui.service.admin.AdminService._call_tool", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = mock_result

        result = await OpsService.reload_patterns()

        assert result["success"] is False
        assert "Reload failed: DB Error" in result["message"]
        mock_call.assert_awaited_once_with("reload_patterns", {})

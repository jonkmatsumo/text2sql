from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from streamlit_app.service.ops_service import OpsService


# Mock ReloadResult since it's imported inside the method
class MockReloadResult:
    """Mock result for pattern reload."""

    def __init__(
        self, success, reload_id="test_id", duration_ms=100.0, pattern_count=10, error=None
    ):
        """Initialize mock result."""
        self.success = success
        self.reload_id = reload_id
        self.duration_ms = duration_ms
        self.pattern_count = pattern_count
        self.error = error
        self.reloaded_at = datetime.utcnow()


@pytest.mark.asyncio
async def test_reload_patterns_success():
    """Test successful pattern reload execution."""
    with patch(
        "mcp_server.services.canonicalization.pattern_reload_service.PatternReloadService.reload",
        new_callable=AsyncMock,
    ) as mock_reload:
        mock_reload.return_value = MockReloadResult(success=True)

        result = await OpsService.reload_patterns()

        assert result["success"] is True
        assert result["message"] == "Patterns reloaded successfully."
        assert result["reload_id"] == "test_id"
        assert result["pattern_count"] == 10
        mock_reload.assert_awaited_once_with(source="admin_ui")


@pytest.mark.asyncio
async def test_reload_patterns_failure():
    """Test failed pattern reload handling."""
    with patch(
        "mcp_server.services.canonicalization.pattern_reload_service.PatternReloadService.reload",
        new_callable=AsyncMock,
    ) as mock_reload:
        mock_reload.return_value = MockReloadResult(success=False, error="DB Error")

        result = await OpsService.reload_patterns()

        assert result["success"] is False
        assert "Reload failed: DB Error" in result["message"]
        assert result["reload_id"] == "test_id"
        mock_reload.assert_awaited_once_with(source="admin_ui")

from datetime import datetime
from unittest.mock import patch

import pytest
from mcp_server.services.canonicalization.pattern_reload_service import ReloadResult
from mcp_server.tools.admin.reload_patterns import handler


@pytest.mark.asyncio
async def test_reload_patterns_tool_success():
    """Test reload_patterns tool success response."""
    with patch("mcp_server.tools.admin.reload_patterns.PatternReloadService.reload") as mock_reload:
        # Setup mock return
        now = datetime.utcnow()
        mock_reload.return_value = ReloadResult(
            success=True, error=None, reloaded_at=now, pattern_count=123
        )

        # Execute tool
        result = await handler()

        # Assert
        assert result["success"] is True
        assert result["error"] is None
        assert result["reloaded_at"] == now.isoformat()
        assert result["pattern_count"] == 123
        mock_reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_reload_patterns_tool_failure():
    """Test reload_patterns tool failure response."""
    with patch("mcp_server.tools.admin.reload_patterns.PatternReloadService.reload") as mock_reload:
        # Setup mock return
        now = datetime.utcnow()
        mock_reload.return_value = ReloadResult(
            success=False, error="Something went wrong", reloaded_at=now, pattern_count=None
        )

        # Execute tool
        result = await handler()

        # Assert
        assert result["success"] is False
        assert result["error"] == "Something went wrong"
        assert result["reloaded_at"] == now.isoformat()
        assert result["pattern_count"] is None
        mock_reload.assert_awaited_once()

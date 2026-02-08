import json
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
            success=True,
            error=None,
            reloaded_at=now,
            pattern_count=123,
            reload_id="abc-123",
            duration_ms=45.6,
        )

        # Execute tool
        result = await handler()

        # Assert
        # Assert
        data = json.loads(result)
        res = data["result"]
        assert res["success"] is True
        assert res["error"] is None
        assert res["reloaded_at"] == now.isoformat()
        assert res["pattern_count"] == 123
        assert res["reload_id"] == "abc-123"
        assert res["duration_ms"] == 45.6
        mock_reload.assert_awaited_once()


@pytest.mark.asyncio
async def test_reload_patterns_tool_failure():
    """Test reload_patterns tool failure response."""
    with patch("mcp_server.tools.admin.reload_patterns.PatternReloadService.reload") as mock_reload:
        # Setup mock return
        now = datetime.utcnow()
        mock_reload.return_value = ReloadResult(
            success=False,
            error="Something went wrong",
            reloaded_at=now,
            pattern_count=None,
            reload_id="err-456",
            duration_ms=12.3,
        )

        # Execute tool
        result = await handler()

        # Assert
        # Assert
        data = json.loads(result)
        res = data["result"]
        assert res["success"] is False
        assert res["error"] == "Something went wrong"
        assert res["reloaded_at"] == now.isoformat()
        assert res["pattern_count"] is None
        assert res["reload_id"] == "err-456"
        assert res["duration_ms"] == 12.3
        mock_reload.assert_awaited_once()

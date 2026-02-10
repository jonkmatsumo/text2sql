"""Security-focused tests for update_cache error handling."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from common.models.tool_envelopes import ToolResponseEnvelope
from mcp_server.tools.update_cache import handler


@pytest.mark.asyncio
async def test_update_cache_error_is_structured_redacted_and_bounded():
    """Internal exceptions should not leak raw text in tool responses."""
    sentinel = "password=supersecret " + ("x" * 5000)

    with (
        patch(
            "mcp_server.tools.update_cache.update_cache_svc",
            new=AsyncMock(side_effect=RuntimeError(sentinel)),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        response_json = await handler(query="q", sql="select 1", tenant_id=1)

    # Must remain parseable as the canonical tool envelope.
    envelope = ToolResponseEnvelope.model_validate(json.loads(response_json))
    assert envelope.error is not None
    assert envelope.error.message == "Failed to update semantic cache."
    assert "password=supersecret" not in envelope.error.message
    assert len(envelope.error.message) <= 2048

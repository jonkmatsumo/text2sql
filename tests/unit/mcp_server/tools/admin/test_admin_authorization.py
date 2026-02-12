"""Tests for admin tool request authorization hardening."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from mcp_server.tools.admin.list_interactions import handler
from mcp_server.utils.request_auth_context import (
    reset_internal_auth_verified,
    set_internal_auth_verified,
)


@pytest.mark.asyncio
async def test_admin_tool_rejects_without_verified_internal_token(monkeypatch):
    """Admin tools require a verified internal token when auth is enabled."""
    monkeypatch.setenv("MCP_USER_ROLE", "ADMIN_ROLE")
    monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "secret-token")

    with patch("mcp_server.tools.admin.list_interactions.get_interaction_store") as mock_get_store:
        result = await handler(limit=10, offset=0)

    data = json.loads(result)
    assert data["error"]["category"] == "unauthorized"
    assert data["error"]["code"] == "UNAUTHORIZED_ADMIN_TOKEN"
    mock_get_store.assert_not_called()


@pytest.mark.asyncio
async def test_admin_tool_allows_verified_internal_token(monkeypatch):
    """Admin tools run when role and internal token context are both present."""
    monkeypatch.setenv("MCP_USER_ROLE", "ADMIN_ROLE")
    monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "secret-token")

    token = set_internal_auth_verified(True)
    try:
        with patch(
            "mcp_server.tools.admin.list_interactions.get_interaction_store"
        ) as mock_get_store:
            mock_store = AsyncMock()
            mock_store.get_recent_interactions = AsyncMock(return_value=[])
            mock_get_store.return_value = mock_store

            result = await handler(limit=5, offset=0)

        data = json.loads(result)
        assert data["result"] == []
        assert data.get("error") is None
        mock_store.get_recent_interactions.assert_called_once_with(5, 0)
    finally:
        reset_internal_auth_verified(token)

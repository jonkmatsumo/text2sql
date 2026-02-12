"""Shared fixtures for admin tool unit tests."""

import pytest


@pytest.fixture(autouse=True)
def _admin_role_defaults(monkeypatch):
    """Set admin-tool tests to an admin role without internal token enforcement."""
    monkeypatch.setenv("MCP_USER_ROLE", "ADMIN_ROLE")
    monkeypatch.delenv("INTERNAL_AUTH_TOKEN", raising=False)

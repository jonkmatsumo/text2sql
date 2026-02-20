"""Unit tests for MCP response envelope helpers."""

import json
from unittest.mock import patch

from mcp_server.utils.envelopes import tool_success_response
from mcp_server.utils.errors import tool_error_response


def test_tool_success_response_uses_query_target_provider_postgres() -> None:
    """Success envelopes should inherit the active query-target provider."""
    with patch("dal.database.Database.get_query_target_provider", return_value="postgres"):
        payload = json.loads(tool_success_response({"ok": True}))

    assert payload["metadata"]["provider"] == "postgres"


def test_tool_success_response_uses_query_target_provider_non_postgres() -> None:
    """Success envelopes should preserve non-Postgres provider names."""
    with patch("dal.database.Database.get_query_target_provider", return_value="snowflake"):
        payload = json.loads(tool_success_response({"ok": True}))

    assert payload["metadata"]["provider"] == "snowflake"


def test_tool_success_response_provider_fallback_is_unspecified() -> None:
    """Provider fallback must be bounded and never emit legacy unknown."""
    with patch("dal.database.Database.get_query_target_provider", return_value="unknown"):
        payload = json.loads(tool_success_response({"ok": True}))

    assert payload["metadata"]["provider"] == "unspecified"
    assert payload["metadata"]["provider"] != "unknown"


def test_tool_error_response_provider_fallback_is_unspecified() -> None:
    """Error envelopes should share the same provider fallback behavior."""
    with patch("dal.database.Database.get_query_target_provider", side_effect=RuntimeError("boom")):
        payload = json.loads(
            tool_error_response(
                message="Tenant is required.",
                code="MISSING_TENANT_ID",
            )
        )

    assert payload["metadata"]["provider"] == "unspecified"
    assert payload["error"]["provider"] == "unspecified"
    assert payload["metadata"]["provider"] != "unknown"

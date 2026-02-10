"""Unit tests for MCP error helper envelopes."""

import json

from mcp_server.utils.errors import tool_error_response


def test_tool_error_response_envelope_shape():
    """Canonical helper should emit typed envelope-compatible fields."""
    response = tool_error_response(
        message="Tenant is required.",
        code="MISSING_TENANT_ID",
        category="invalid_request",
        provider="mcp_server",
        retryable=False,
    )

    payload = json.loads(response)
    assert payload["schema_version"] == "1.0"
    assert payload["metadata"]["provider"] == "mcp_server"
    assert payload["error"]["message"] == "Tenant is required."
    assert payload["error"]["sql_state"] == "MISSING_TENANT_ID"
    assert payload["error"]["category"] == "invalid_request"
    assert payload["error"]["provider"] == "mcp_server"
    assert payload["error"]["is_retryable"] is False

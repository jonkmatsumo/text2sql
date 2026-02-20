"""Contract tests for canonical MCP tool error envelopes."""

from __future__ import annotations

import json

import pytest

from common.errors.error_codes import ErrorCode, parse_error_code


@pytest.mark.parametrize(
    "tool_name,invoker",
    [
        (
            "submit_feedback",
            lambda: __import__(
                "mcp_server.tools.feedback.submit_feedback",
                fromlist=["handler"],
            ).handler("", "UP", tenant_id=1),
        ),
        (
            "resolve_ambiguity",
            lambda: __import__(
                "mcp_server.tools.resolve_ambiguity",
                fromlist=["handler"],
            ).handler("revenue", "invalid-schema-context", tenant_id=1),
        ),
        (
            "get_interaction_details",
            lambda: __import__(
                "mcp_server.tools.admin.get_interaction_details",
                fromlist=["handler"],
            ).handler("interaction-1"),
        ),
    ],
)
@pytest.mark.asyncio
async def test_failing_tools_emit_canonical_error_envelope(tool_name, invoker, monkeypatch):
    """Failing tools should always return the canonical structured error envelope."""
    monkeypatch.setenv("MCP_USER_ROLE", "")
    raw = await invoker()
    payload = json.loads(raw)

    assert payload["schema_version"] == "1.0"
    assert "metadata" in payload
    assert isinstance(payload["metadata"], dict)

    error = payload.get("error")
    assert isinstance(error, dict), f"{tool_name} must include error object"
    assert isinstance(error.get("category"), str) and error["category"]
    assert isinstance(error.get("code"), str) and error["code"]
    assert isinstance(error.get("error_code"), str) and error["error_code"]
    assert isinstance(error.get("message"), str) and error["message"]
    assert isinstance(error.get("retryable"), bool)
    assert parse_error_code(error["error_code"]) in set(ErrorCode)

    # Legacy aliases remain present for compatibility while migration is ongoing.
    assert error.get("sql_state") == error.get("code")
    assert error.get("is_retryable") == error.get("retryable")

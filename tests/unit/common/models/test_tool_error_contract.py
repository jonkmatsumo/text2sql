"""Tests for canonical tool error/envelope contract."""

from common.models.error_metadata import ToolError
from common.models.tool_envelopes import ToolResponseEnvelope
from common.models.tool_errors import (
    tool_error_internal,
    tool_error_invalid_request,
    tool_error_timeout,
    tool_error_unsupported_capability,
)


def test_tool_error_accepts_legacy_keys():
    """Legacy error keys should normalize into canonical ToolError fields."""
    err = ToolError(
        category="invalid_request",
        sql_state="BAD_INPUT",
        message="Bad input.",
        is_retryable=False,
        retry_reason="bad_input",
        provider="mcp_server",
    )
    assert err.code == "BAD_INPUT"
    assert err.retryable is False
    assert err.reason_code == "bad_input"


def test_tool_error_serializes_canonical_and_legacy_fields():
    """Serialized errors should include canonical and legacy keys."""
    err = ToolError(
        category="timeout",
        code="TIMEOUT",
        message="Timed out.",
        retryable=True,
        reason_code="timeout",
        provider="postgres",
    )
    payload = err.model_dump(exclude_none=True)
    assert payload["code"] == "TIMEOUT"
    assert payload["retryable"] is True
    assert payload["reason_code"] == "timeout"
    assert payload["sql_state"] == "TIMEOUT"
    assert payload["is_retryable"] is True
    assert payload["retry_reason"] == "timeout"


def test_tool_response_envelope_round_trip():
    """Validate that ToolResponseEnvelope preserves canonical errors."""
    err = tool_error_invalid_request(
        code="INVALID_LIMIT",
        message="Limit is invalid.",
        reason_code="limit_invalid",
        provider="mcp_server",
    )
    envelope = ToolResponseEnvelope(
        result={"ok": False},
        metadata={"provider": "mcp_server"},
        error=err,
    )
    hydrated = ToolResponseEnvelope.model_validate(envelope.model_dump())
    assert hydrated.error is not None
    assert hydrated.error.code == "INVALID_LIMIT"
    assert hydrated.error.retryable is False
    assert hydrated.error.message == "Limit is invalid."


def test_tool_error_helper_defaults():
    """Helper constructors should set expected categories/retryability defaults."""
    unsupported = tool_error_unsupported_capability(message="Not supported.")
    internal = tool_error_internal()
    timeout = tool_error_timeout()

    assert unsupported.category == "unsupported_capability"
    assert unsupported.retryable is False
    assert internal.category == "internal_error"
    assert internal.retryable is False
    assert timeout.category == "timeout"
    assert timeout.retryable is True

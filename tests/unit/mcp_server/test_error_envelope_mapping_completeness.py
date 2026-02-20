"""Completeness checks for canonical error-code coverage in MCP envelopes."""

from __future__ import annotations

import json

import pytest

from common.errors.error_codes import ErrorCode, canonical_error_code_for_category
from common.models.error_metadata import ErrorCategory
from common.models.tool_envelopes import parse_execute_sql_response
from mcp_server.utils.contract_enforcement import _build_malformed_envelope
from mcp_server.utils.errors import tool_error_response


@pytest.mark.parametrize(
    ("category", "expected_code"),
    [
        (ErrorCategory.INVALID_REQUEST, ErrorCode.VALIDATION_ERROR),
        (ErrorCategory.TIMEOUT, ErrorCode.DB_TIMEOUT),
        (
            ErrorCategory.TENANT_ENFORCEMENT_UNSUPPORTED,
            ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED,
        ),
    ],
)
def test_tool_error_response_always_sets_canonical_error_code(
    category: ErrorCategory, expected_code: ErrorCode
) -> None:
    """Known MCP envelope constructor should always include canonical error_code."""
    payload = json.loads(
        tool_error_response(
            message="boom",
            code="TEST_ERROR",
            category=category,
        )
    )

    assert payload["error"]["error_code"] == expected_code.value


def test_contract_enforcement_malformed_error_has_canonical_error_code() -> None:
    """Malformed envelope fallback should include canonical error_code."""
    payload = json.loads(
        _build_malformed_envelope(
            tool_name="test_tool",
            payload_kind="raw_string",
            parse_error_type="JSONDecodeError",
        )
    )

    assert (
        payload["error"]["error_code"]
        == canonical_error_code_for_category(ErrorCategory.TOOL_RESPONSE_MALFORMED).value
    )


def test_execute_response_parser_malformed_payload_has_canonical_error_code() -> None:
    """Parser-generated malformed execute envelope should include canonical error_code."""
    envelope = parse_execute_sql_response("not-json")
    error = envelope.error
    assert error is not None
    assert error.error_code == canonical_error_code_for_category("tool_response_malformed").value

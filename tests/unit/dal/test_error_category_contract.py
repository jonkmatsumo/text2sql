"""Contract tests for ErrorCategory enum and ToolError model."""

import pytest
from pydantic import ValidationError

from common.models.error_metadata import ErrorCategory, ToolError


def test_error_category_enum_values():
    """Ensure ErrorCategory enum has expected string values for contract stability."""
    assert ErrorCategory.AUTH == "auth"
    assert ErrorCategory.INVALID_REQUEST == "invalid_request"
    assert ErrorCategory.TIMEOUT == "timeout"
    assert ErrorCategory.SYNTAX == "syntax"
    assert ErrorCategory.INTERNAL == "internal"
    assert ErrorCategory.UNKNOWN == "unknown"


def test_tool_error_accepts_enum_category():
    """Ensure ToolError accepts ErrorCategory enum members."""
    err = ToolError(category=ErrorCategory.AUTH, message="Unauthorized access", provider="postgres")
    assert err.category == ErrorCategory.AUTH
    assert err.category == "auth"


def test_tool_error_serialization_contains_string_category():
    """Ensure ToolError serialization converts enum to string for backward compatibility."""
    err = ToolError(category=ErrorCategory.TIMEOUT, message="Query timed out", provider="athena")
    dump = err.model_dump()
    assert dump["category"] == "timeout"

    # Check JSON serialization
    json_dump = err.model_dump_json()
    assert '"category":"timeout"' in json_dump


def test_tool_error_validation_rejects_invalid_category_strings():
    """Ensure ToolError rejects strings not in the ErrorCategory enum."""
    with pytest.raises(ValidationError):
        ToolError(
            category="invalid_category_string_that_does_not_exist", message="Test", provider="test"
        )


def test_tool_error_normalization_handles_legacy_aliases():
    """Ensure legacy aliases (sql_state, is_retryable) are correctly normalized and emitted."""
    data = {
        "category": ErrorCategory.INVALID_REQUEST,
        "message": "Invalid SQL",
        "sql_state": "42601",
        "is_retryable": False,
    }
    err = ToolError.model_validate(data)
    assert err.code == "42601"
    assert err.retryable is False

    # Serialization should include both canonical and legacy keys
    dump = err.to_dict()
    assert dump["code"] == "42601"
    assert dump["sql_state"] == "42601"
    assert dump.get("retryable") is False
    assert dump.get("is_retryable") is False

"""Tests for canonical error-code taxonomy helpers."""

from common.errors.error_codes import ErrorCode, canonical_error_code_for_category, error_code_group
from common.models.error_metadata import ErrorCategory


def test_error_code_enum_values_are_stable():
    """Enum values should stay stable because external consumers depend on them."""
    assert ErrorCode.VALIDATION_ERROR.value == "VALIDATION_ERROR"
    assert ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED.value == "TENANT_ENFORCEMENT_UNSUPPORTED"
    assert ErrorCode.READONLY_VIOLATION.value == "READONLY_VIOLATION"
    assert ErrorCode.SQL_POLICY_VIOLATION.value == "SQL_POLICY_VIOLATION"
    assert ErrorCode.DB_CONNECTION_ERROR.value == "DB_CONNECTION_ERROR"
    assert ErrorCode.DB_TIMEOUT.value == "DB_TIMEOUT"
    assert ErrorCode.DB_SYNTAX_ERROR.value == "DB_SYNTAX_ERROR"
    assert ErrorCode.AMBIGUITY_UNRESOLVED.value == "AMBIGUITY_UNRESOLVED"
    assert ErrorCode.INTERNAL_ERROR.value == "INTERNAL_ERROR"


def test_category_to_canonical_error_code_mapping():
    """Known categories should map to the expected canonical codes."""
    assert (
        canonical_error_code_for_category(ErrorCategory.INVALID_REQUEST)
        == ErrorCode.VALIDATION_ERROR
    )
    assert canonical_error_code_for_category(ErrorCategory.TIMEOUT) == ErrorCode.DB_TIMEOUT
    assert (
        canonical_error_code_for_category(ErrorCategory.CONNECTIVITY)
        == ErrorCode.DB_CONNECTION_ERROR
    )
    assert (
        canonical_error_code_for_category(ErrorCategory.TENANT_ENFORCEMENT_UNSUPPORTED)
        == ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED
    )
    assert canonical_error_code_for_category("does-not-exist") == ErrorCode.INTERNAL_ERROR


def test_error_code_groups_are_stable():
    """Canonical error groups should remain bounded and deterministic."""
    assert error_code_group(ErrorCode.DB_TIMEOUT) == "DB"
    assert error_code_group(ErrorCode.TENANT_ENFORCEMENT_UNSUPPORTED) == "TENANT"
    assert error_code_group(ErrorCode.READONLY_VIOLATION) == "POLICY"
    assert error_code_group("INVALID_VALUE") == "INTERNAL"

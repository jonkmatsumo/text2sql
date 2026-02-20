"""Tests for user-facing error message sanitization."""

from common.errors.error_codes import ErrorCode
from common.errors.sanitization import sanitize_error_message, sanitize_exception


def test_sanitize_message_removes_relation_identifier_details():
    """Quoted relation/identifier details should not leak in user-facing output."""
    message = 'relation "users" does not exist'
    safe = sanitize_error_message(message, error_code=ErrorCode.DB_SYNTAX_ERROR.value)

    assert safe == "relation <redacted_identifier> does not exist"
    assert "users" not in safe.lower()


def test_sanitize_message_removes_sql_fragments_from_validation_errors():
    """Rendered SQL snippets should be collapsed to bounded safe text."""
    message = "SQL execution failed near SELECT * FROM payroll WHERE ssn = '123-45-6789'"
    safe = sanitize_error_message(message, error_code=ErrorCode.VALIDATION_ERROR.value)

    assert safe == "SQL validation failed."
    assert "select * from payroll" not in safe.lower()


def test_sanitize_exception_uses_canonical_template():
    """Exception sanitization should use stable templates for canonical codes."""

    class _DatabaseTimeout(RuntimeError):
        pass

    safe = sanitize_exception(
        _DatabaseTimeout("statement timeout: SELECT * FROM customer"),
        error_code=ErrorCode.DB_TIMEOUT.value,
    )

    assert safe == "Database connection timed out."

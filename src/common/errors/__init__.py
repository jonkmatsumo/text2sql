"""Common error taxonomy helpers."""

from common.errors.error_codes import ErrorCode, canonical_error_code_for_category, error_code_group
from common.errors.sanitization import sanitize_error_message, sanitize_exception

__all__ = [
    "ErrorCode",
    "canonical_error_code_for_category",
    "error_code_group",
    "sanitize_error_message",
    "sanitize_exception",
]

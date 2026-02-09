from enum import Enum


class TerminationReason(str, Enum):
    """Reason for agent workflow termination."""

    SUCCESS = "success"
    BUDGET_EXHAUSTED = "budget_exhausted"
    SCHEMA_CHANGED = "schema_changed"
    PERMISSION_DENIED = "permission_denied"
    READONLY_VIOLATION = "readonly_violation"
    TIMEOUT = "timeout"
    VALIDATION_FAILED = "validation_failed"
    UNKNOWN = "unknown"

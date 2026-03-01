"""Request-scoped execution budget helpers for paginated SQL flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dal.execution_resource_limits import ExecutionResourceLimits

PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED = "PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED"
PAGINATION_GLOBAL_BYTE_BUDGET_EXCEEDED = "PAGINATION_GLOBAL_BYTE_BUDGET_EXCEEDED"
PAGINATION_GLOBAL_TIME_BUDGET_EXCEEDED = "PAGINATION_GLOBAL_TIME_BUDGET_EXCEEDED"
PAGINATION_BUDGET_SNAPSHOT_INVALID = "PAGINATION_BUDGET_SNAPSHOT_INVALID"

_BUDGET_INT_MAX = 2_147_483_647


class ExecutionBudgetError(ValueError):
    """Base class for execution-budget validation/enforcement failures."""

    def __init__(self, message: str, *, reason_code: str) -> None:
        """Attach a stable reason code to the error instance."""
        super().__init__(message)
        self.reason_code = reason_code


class ExecutionBudgetSnapshotError(ExecutionBudgetError):
    """Raised when a cursor budget snapshot is missing/malformed."""

    def __init__(self, message: str = "Invalid pagination budget snapshot.") -> None:
        """Initialize with the canonical snapshot-validation reason code."""
        super().__init__(message, reason_code=PAGINATION_BUDGET_SNAPSHOT_INVALID)


class ExecutionBudgetExceededError(ExecutionBudgetError):
    """Raised when accumulated usage exceeds a request-scoped budget."""


def _parse_strict_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExecutionBudgetSnapshotError(
            f"Invalid pagination budget snapshot: {field_name} must be an integer."
        )
    if value < 0:
        raise ExecutionBudgetSnapshotError(
            f"Invalid pagination budget snapshot: {field_name} must be non-negative."
        )
    if value > _BUDGET_INT_MAX:
        raise ExecutionBudgetSnapshotError(
            f"Invalid pagination budget snapshot: {field_name} exceeds bounded range."
        )
    return int(value)


def _parse_positive_int(value: Any, *, field_name: str) -> int:
    parsed = _parse_strict_int(value, field_name=field_name)
    if parsed <= 0:
        raise ExecutionBudgetSnapshotError(
            f"Invalid pagination budget snapshot: {field_name} must be greater than zero."
        )
    return parsed


@dataclass(frozen=True)
class ExecutionBudget:
    """Composite request-level execution budget and consumed counters."""

    max_total_rows: int
    max_total_bytes: int
    max_total_duration_ms: int
    consumed_rows: int = 0
    consumed_bytes: int = 0
    consumed_duration_ms: int = 0

    @classmethod
    def from_resource_limits(cls, resource_limits: ExecutionResourceLimits) -> "ExecutionBudget":
        """Build a request-scoped budget from validated resource limits."""
        max_total_rows = (
            _parse_positive_int(resource_limits.max_rows, field_name="max_total_rows")
            if resource_limits.enforce_row_limit
            else _BUDGET_INT_MAX
        )
        max_total_bytes = (
            _parse_positive_int(resource_limits.max_bytes, field_name="max_total_bytes")
            if resource_limits.enforce_byte_limit
            else _BUDGET_INT_MAX
        )
        max_total_duration_ms = (
            _parse_positive_int(
                resource_limits.max_execution_ms,
                field_name="max_total_duration_ms",
            )
            if resource_limits.enforce_timeout
            else _BUDGET_INT_MAX
        )
        return cls(
            max_total_rows=max_total_rows,
            max_total_bytes=max_total_bytes,
            max_total_duration_ms=max_total_duration_ms,
            consumed_rows=0,
            consumed_bytes=0,
            consumed_duration_ms=0,
        )

    @classmethod
    def from_snapshot(cls, snapshot: Any) -> "ExecutionBudget":
        """Parse and validate a cursor budget snapshot (fail-closed)."""
        if not isinstance(snapshot, dict):
            raise ExecutionBudgetSnapshotError()
        required_fields = (
            "max_total_rows",
            "max_total_bytes",
            "max_total_duration_ms",
            "consumed_rows",
            "consumed_bytes",
            "consumed_duration_ms",
        )
        missing = [field for field in required_fields if field not in snapshot]
        if missing:
            raise ExecutionBudgetSnapshotError(
                "Invalid pagination budget snapshot: missing required fields."
            )

        budget = cls(
            max_total_rows=_parse_positive_int(
                snapshot.get("max_total_rows"), field_name="max_total_rows"
            ),
            max_total_bytes=_parse_positive_int(
                snapshot.get("max_total_bytes"), field_name="max_total_bytes"
            ),
            max_total_duration_ms=_parse_positive_int(
                snapshot.get("max_total_duration_ms"),
                field_name="max_total_duration_ms",
            ),
            consumed_rows=_parse_strict_int(
                snapshot.get("consumed_rows"), field_name="consumed_rows"
            ),
            consumed_bytes=_parse_strict_int(
                snapshot.get("consumed_bytes"), field_name="consumed_bytes"
            ),
            consumed_duration_ms=_parse_strict_int(
                snapshot.get("consumed_duration_ms"),
                field_name="consumed_duration_ms",
            ),
        )
        budget.validate()
        return budget

    def validate(self) -> None:
        """Validate internal consistency and bounded integer ranges."""
        if self.consumed_rows > self.max_total_rows:
            raise ExecutionBudgetSnapshotError(
                "Invalid pagination budget snapshot: consumed_rows exceeds max_total_rows."
            )
        if self.consumed_bytes > self.max_total_bytes:
            raise ExecutionBudgetSnapshotError(
                "Invalid pagination budget snapshot: consumed_bytes exceeds max_total_bytes."
            )
        if self.consumed_duration_ms > self.max_total_duration_ms:
            raise ExecutionBudgetSnapshotError(
                "Invalid pagination budget snapshot: consumed_duration_ms "
                "exceeds max_total_duration_ms."
            )

    def to_snapshot(self) -> dict[str, int]:
        """Serialize to a bounded cursor-safe snapshot."""
        return {
            "max_total_rows": int(self.max_total_rows),
            "max_total_bytes": int(self.max_total_bytes),
            "max_total_duration_ms": int(self.max_total_duration_ms),
            "consumed_rows": int(self.consumed_rows),
            "consumed_bytes": int(self.consumed_bytes),
            "consumed_duration_ms": int(self.consumed_duration_ms),
        }

    @property
    def rows_remaining(self) -> int:
        """Remaining row budget for the current request chain."""
        return max(0, int(self.max_total_rows) - int(self.consumed_rows))

    @property
    def bytes_remaining(self) -> int:
        """Remaining byte budget for the current request chain."""
        return max(0, int(self.max_total_bytes) - int(self.consumed_bytes))

    @property
    def duration_remaining_ms(self) -> int:
        """Remaining execution-time budget in milliseconds."""
        return max(0, int(self.max_total_duration_ms) - int(self.consumed_duration_ms))

    def exhausted_reason_code(self) -> str | None:
        """Return deterministic reason code when any dimension is exhausted."""
        if self.consumed_rows >= self.max_total_rows:
            return PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED
        if self.consumed_bytes >= self.max_total_bytes:
            return PAGINATION_GLOBAL_BYTE_BUDGET_EXCEEDED
        if self.consumed_duration_ms >= self.max_total_duration_ms:
            return PAGINATION_GLOBAL_TIME_BUDGET_EXCEEDED
        return None

    def consume(
        self,
        *,
        rows: int,
        bytes_returned: int,
        duration_ms: int,
    ) -> "ExecutionBudget":
        """Return a new budget with page consumption applied, or raise on overflow."""
        rows_delta = _parse_strict_int(rows, field_name="rows")
        bytes_delta = _parse_strict_int(bytes_returned, field_name="bytes_returned")
        duration_delta = _parse_strict_int(duration_ms, field_name="duration_ms")

        next_rows = int(self.consumed_rows) + rows_delta
        if next_rows > int(self.max_total_rows):
            raise ExecutionBudgetExceededError(
                "Pagination request exceeded the global row budget.",
                reason_code=PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED,
            )
        next_bytes = int(self.consumed_bytes) + bytes_delta
        if next_bytes > int(self.max_total_bytes):
            raise ExecutionBudgetExceededError(
                "Pagination request exceeded the global byte budget.",
                reason_code=PAGINATION_GLOBAL_BYTE_BUDGET_EXCEEDED,
            )
        next_duration = int(self.consumed_duration_ms) + duration_delta
        if next_duration > int(self.max_total_duration_ms):
            raise ExecutionBudgetExceededError(
                "Pagination request exceeded the global time budget.",
                reason_code=PAGINATION_GLOBAL_TIME_BUDGET_EXCEEDED,
            )

        return ExecutionBudget(
            max_total_rows=int(self.max_total_rows),
            max_total_bytes=int(self.max_total_bytes),
            max_total_duration_ms=int(self.max_total_duration_ms),
            consumed_rows=next_rows,
            consumed_bytes=next_bytes,
            consumed_duration_ms=next_duration,
        )

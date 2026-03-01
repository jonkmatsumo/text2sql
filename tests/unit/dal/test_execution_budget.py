"""Tests for request-scoped execution budget parsing and enforcement."""

import pytest

from dal.execution_budget import (
    PAGINATION_BUDGET_SNAPSHOT_INVALID,
    PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED,
    ExecutionBudget,
    ExecutionBudgetExceededError,
    ExecutionBudgetSnapshotError,
)


def _valid_snapshot() -> dict[str, int]:
    return {
        "max_total_rows": 100,
        "max_total_bytes": 1_000_000,
        "max_total_duration_ms": 60_000,
        "consumed_rows": 25,
        "consumed_bytes": 500,
        "consumed_duration_ms": 1_000,
    }


def test_execution_budget_from_snapshot_fail_closed_on_missing_required_field():
    """Missing budget fields should reject continuation snapshots."""
    snapshot = _valid_snapshot()
    snapshot.pop("max_total_rows")

    with pytest.raises(ExecutionBudgetSnapshotError) as exc_info:
        ExecutionBudget.from_snapshot(snapshot)

    assert exc_info.value.reason_code == PAGINATION_BUDGET_SNAPSHOT_INVALID


def test_execution_budget_from_snapshot_rejects_negative_values():
    """Negative values in a snapshot should fail closed."""
    snapshot = _valid_snapshot()
    snapshot["consumed_rows"] = -1

    with pytest.raises(ExecutionBudgetSnapshotError) as exc_info:
        ExecutionBudget.from_snapshot(snapshot)

    assert exc_info.value.reason_code == PAGINATION_BUDGET_SNAPSHOT_INVALID


def test_execution_budget_from_snapshot_rejects_overflow_values():
    """Snapshot values above bounded integer range should fail closed."""
    snapshot = _valid_snapshot()
    snapshot["max_total_bytes"] = 2_147_483_648

    with pytest.raises(ExecutionBudgetSnapshotError) as exc_info:
        ExecutionBudget.from_snapshot(snapshot)

    assert exc_info.value.reason_code == PAGINATION_BUDGET_SNAPSHOT_INVALID


def test_execution_budget_consume_allows_exact_threshold_and_marks_exhausted():
    """Hitting the row ceiling exactly is allowed and marks the budget exhausted."""
    budget = ExecutionBudget(
        max_total_rows=100,
        max_total_bytes=10_000,
        max_total_duration_ms=10_000,
        consumed_rows=80,
        consumed_bytes=1_000,
        consumed_duration_ms=500,
    )

    updated = budget.consume(rows=20, bytes_returned=100, duration_ms=50)

    assert updated.consumed_rows == 100
    assert updated.exhausted_reason_code() == PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED


def test_execution_budget_consume_rejects_when_row_budget_exceeded():
    """Crossing the row ceiling should reject with the stable row budget reason code."""
    budget = ExecutionBudget(
        max_total_rows=100,
        max_total_bytes=10_000,
        max_total_duration_ms=10_000,
        consumed_rows=80,
        consumed_bytes=1_000,
        consumed_duration_ms=500,
    )

    with pytest.raises(ExecutionBudgetExceededError) as exc_info:
        budget.consume(rows=21, bytes_returned=100, duration_ms=50)

    assert exc_info.value.reason_code == PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED

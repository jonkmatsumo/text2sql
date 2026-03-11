"""Tests for request-scoped execution budget parsing and enforcement."""

import pytest

from dal.execution_budget import (
    PAGINATION_BUDGET_SNAPSHOT_INVALID,
    PAGINATION_GLOBAL_ROW_BUDGET_EXCEEDED,
    ExecutionBudget,
    ExecutionBudgetExceededError,
    ExecutionBudgetSnapshotError,
    compute_effective_page_size,
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


def test_compute_effective_page_size_respects_remaining_row_budget() -> None:
    """Row budget should cap effective page size deterministically."""
    assert (
        compute_effective_page_size(
            requested_page_size=100,
            remaining_row_budget=25,
            remaining_byte_budget=None,
            estimated_avg_row_bytes=None,
        )
        == 25
    )


def test_compute_effective_page_size_respects_remaining_byte_budget() -> None:
    """Byte budget + row estimate should cap effective page size deterministically."""
    assert (
        compute_effective_page_size(
            requested_page_size=100,
            remaining_row_budget=None,
            remaining_byte_budget=1_000,
            estimated_avg_row_bytes=100,
        )
        == 10
    )


def test_compute_effective_page_size_returns_zero_when_no_safe_page() -> None:
    """Zero remaining budget should fail closed with no safe page."""
    assert (
        compute_effective_page_size(
            requested_page_size=100,
            remaining_row_budget=0,
            remaining_byte_budget=None,
            estimated_avg_row_bytes=None,
        )
        == 0
    )


def test_compute_effective_page_size_missing_byte_estimate_is_deterministic() -> None:
    """Missing byte estimate must fall back to row-budget-only sizing."""
    first = compute_effective_page_size(
        requested_page_size=100,
        remaining_row_budget=25,
        remaining_byte_budget=10,
        estimated_avg_row_bytes=None,
    )
    second = compute_effective_page_size(
        requested_page_size=100,
        remaining_row_budget=25,
        remaining_byte_budget=10,
        estimated_avg_row_bytes=None,
    )
    assert first == 25
    assert second == 25

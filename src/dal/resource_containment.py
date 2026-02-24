"""Execution-boundary result containment helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from common.constants.reason_codes import PayloadTruncationReason


@dataclass(frozen=True)
class RowContainmentResult:
    """Result of hard row-limit enforcement."""

    rows: list[dict[str, Any]]
    partial: bool
    partial_reason: str | None
    items_returned: int


def enforce_row_limit(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_rows: int,
    enforce: bool,
) -> RowContainmentResult:
    """Apply a deterministic hard row cap while preserving row order."""
    bounded_rows: list[dict[str, Any]] = []
    if not enforce or max_rows <= 0:
        for row in rows:
            bounded_rows.append(dict(row))
        return RowContainmentResult(
            rows=bounded_rows,
            partial=False,
            partial_reason=None,
            items_returned=len(bounded_rows),
        )

    count = 0
    truncated = False
    for row in rows:
        if count >= max_rows:
            truncated = True
            break
        bounded_rows.append(dict(row))
        count += 1

    return RowContainmentResult(
        rows=bounded_rows,
        partial=truncated,
        partial_reason=PayloadTruncationReason.MAX_ROWS.value if truncated else None,
        items_returned=len(bounded_rows),
    )

"""Shared helpers for pagination cursor time-window metadata."""

from __future__ import annotations

import time
from typing import Any


def cursor_now_epoch_seconds(*, now_epoch_seconds: int | None = None) -> int:
    """Return current unix epoch seconds from a single cursor clock source."""
    if now_epoch_seconds is None:
        return int(time.time())
    return int(now_epoch_seconds)


def normalize_optional_int(value: Any) -> int | None:
    """Normalize optional integers while rejecting booleans and empty values."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_strict_int(value: Any) -> int | None:
    """Return integers only when the raw value is an int (excluding bool)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def bounded_cursor_age_seconds(age_seconds: int, *, max_bound: int = 604_800) -> int:
    """Clamp cursor age to bounded telemetry-friendly integer ranges."""
    if age_seconds < 0:
        return 0
    return min(int(age_seconds), int(max_bound))

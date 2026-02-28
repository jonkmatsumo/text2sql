"""Shared helpers for pagination cursor time-window metadata.

Cursor Guardrail Semantics
--------------------------
Pagination cursors (offset and keyset) embed time-window metadata and are
validated on decode with the following rules:

TTL
  Each cursor stamps ``issued_at`` (unix epoch seconds) on encode.  On decode
  the age is checked against ``max_age_seconds`` (default 3600 s / 1 hour,
  configurable via ``PAGINATION_CURSOR_MAX_AGE_SECONDS``).  Cursors older
  than the effective TTL are rejected with ``PAGINATION_CURSOR_EXPIRED``.

Clock-skew tolerance
  A cursor whose ``issued_at`` is in the future beyond ``clock_skew_seconds``
  (default 300 s / 5 min, configurable via
  ``PAGINATION_CURSOR_CLOCK_SKEW_SECONDS``) is rejected with
  ``PAGINATION_CURSOR_CLOCK_SKEW``.

Query fingerprint binding
  When ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT`` is enabled (default True
  for keyset, False for offset) a SHA-256 ``query_fp`` is embedded in the
  cursor.  On decode the caller's recomputed fingerprint must match;
  mismatches are rejected with ``PAGINATION_CURSOR_QUERY_MISMATCH``.

Failure reason codes (bounded set)
  Every decode failure maps to exactly one of the following reason codes so
  that telemetry and error responses remain deterministic and bounded:

  - ``PAGINATION_CURSOR_EXPIRED``          — TTL exceeded
  - ``PAGINATION_CURSOR_CLOCK_SKEW``       — issued_at too far in the future
  - ``PAGINATION_CURSOR_ISSUED_AT_INVALID``— missing or non-integer issued_at
  - ``PAGINATION_CURSOR_QUERY_MISMATCH``   — query fingerprint mismatch
  - ``KEYSET_ORDER_MISMATCH``              — ORDER BY key drift
  - ``KEYSET_SNAPSHOT_MISMATCH``           — cursor context drift (snapshot)
  - ``KEYSET_TOPOLOGY_MISMATCH``           — cursor context drift (topology)
  - ``KEYSET_SHARD_MISMATCH``              — cursor context drift (shard)
  - ``KEYSET_PARTITION_SET_CHANGED``       — cursor context drift (partition)
  - ``PAGINATION_BACKEND_SET_CHANGED``     — backend set signature changed

Environment variables (resolved at the MCP handler layer)
  ``PAGINATION_CURSOR_MAX_AGE_SECONDS``          — int, default 3600
  ``PAGINATION_CURSOR_CLOCK_SKEW_SECONDS``       — int, default 300
  ``PAGINATION_CURSOR_REQUIRE_ISSUED_AT``        — bool, default True
  ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT``   — bool, default True (keyset) / False (offset)
  ``EXECUTION_PAGINATION_TOKEN_SECRET``          — str, default "" (signing disabled)
  ``EXECUTION_PAGINATION_TOKEN_MAX_LENGTH``       — int, default per-module constant

All env vars have safe defaults and are validated fail-closed: unset or
unparseable values fall back to the safe default (e.g. TTL=3600, skew=300,
require_issued_at=True).
"""

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

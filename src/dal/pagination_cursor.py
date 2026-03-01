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

Cursor signing
  Cursor HMAC signing is **fail-closed by default**: if
  ``PAGINATION_CURSOR_SIGNING_SECRET`` is not set and
  ``PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET`` is not ``true``, any
  pagination operation that requires a cursor will be rejected with
  ``PAGINATION_CURSOR_SECRET_MISSING``.  Signature mismatches on decode
  produce ``PAGINATION_CURSOR_SIGNATURE_INVALID``.

Environment variables (resolved at the MCP handler layer)
  ``PAGINATION_CURSOR_MAX_AGE_SECONDS``              — int, default 3600
  ``PAGINATION_CURSOR_CLOCK_SKEW_SECONDS``           — int, default 300
  ``PAGINATION_CURSOR_REQUIRE_ISSUED_AT``            — bool, default True
  ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT``       — bool, default True (keyset) / False (offset)
  ``PAGINATION_CURSOR_SIGNING_SECRET``               — str, required (fail-closed)
  ``PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET``    — bool, default False
  ``EXECUTION_PAGINATION_TOKEN_MAX_LENGTH``           — int, default per-module constant

All env vars have safe defaults and are validated fail-closed: unset or
unparseable values fall back to the safe default (e.g. TTL=3600, skew=300,
require_issued_at=True).
"""

from __future__ import annotations

import os
import time
from typing import Any

PAGINATION_CURSOR_SECRET_MISSING = "PAGINATION_CURSOR_SECRET_MISSING"
PAGINATION_CURSOR_SIGNATURE_INVALID = "PAGINATION_CURSOR_SIGNATURE_INVALID"


class CursorSigningSecretMissing(ValueError):
    """Raised when cursor signing secret is not configured and insecure mode is disabled."""

    def __init__(self) -> None:
        """Initialize with the standard fail-closed reason code and guidance message."""
        super().__init__(
            f"{PAGINATION_CURSOR_SECRET_MISSING}: Cursor signing secret is not configured. "
            "Set PAGINATION_CURSOR_SIGNING_SECRET or set "
            "PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET=true for development."
        )
        self.reason_code = PAGINATION_CURSOR_SECRET_MISSING


def resolve_cursor_signing_secret(
    *,
    allow_unsigned: bool | None = None,
) -> str | None:
    """Resolve the cursor signing secret from environment configuration.

    Fail-closed: raises ``CursorSigningSecretMissing`` when no secret is
    configured and unsigned cursors are not explicitly allowed.

    Returns the secret string when configured, or ``None`` when unsigned
    mode is explicitly opted in via ``allow_unsigned=True`` or the
    ``PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET`` env var.
    """
    secret = (os.environ.get("PAGINATION_CURSOR_SIGNING_SECRET") or "").strip()
    if secret:
        return secret
    if allow_unsigned is None:
        raw = (os.environ.get("PAGINATION_CURSOR_ALLOW_INSECURE_DEV_SECRET") or "").strip().lower()
        allow_unsigned = raw in ("true", "1", "yes", "on")
    if allow_unsigned:
        return None
    raise CursorSigningSecretMissing()


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

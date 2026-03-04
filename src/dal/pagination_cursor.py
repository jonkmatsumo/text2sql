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
  - ``KEYSET_CURSOR_ORDERBY_MISMATCH``     — ORDER BY key drift
  - ``KEYSET_SNAPSHOT_MISMATCH``           — cursor context drift (snapshot)
  - ``KEYSET_TOPOLOGY_MISMATCH``           — cursor context drift (topology)
  - ``KEYSET_SHARD_MISMATCH``              — cursor context drift (shard)
  - ``KEYSET_PARTITION_SET_CHANGED``       — cursor context drift (partition)
  - ``PAGINATION_BACKEND_SET_CHANGED``     — backend set signature changed

Cursor signing
  Cursor HMAC signing is **fail-closed by default**: if
  ``PAGINATION_CURSOR_HMAC_SECRET`` (preferred) or
  ``PAGINATION_CURSOR_SIGNING_SECRET`` (legacy compatibility alias) is not set,
  pagination operations that require cursor encode/decode are rejected with
  ``PAGINATION_CURSOR_SECRET_MISSING``.
  If a secret is configured but weaker than the minimum accepted length,
  operations reject with ``PAGINATION_CURSOR_SECRET_WEAK``.
  Signature mismatches on decode produce ``PAGINATION_CURSOR_SIGNATURE_INVALID``.

Environment variables (resolved at the MCP handler layer)
  ``PAGINATION_CURSOR_MAX_AGE_SECONDS``              — int, default 3600
  ``PAGINATION_CURSOR_CLOCK_SKEW_SECONDS``           — int, default 300
  ``PAGINATION_CURSOR_REQUIRE_ISSUED_AT``            — bool, default True
  ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT``       — bool, default True (keyset) / False (offset)
  ``PAGINATION_CURSOR_HMAC_SECRET``                  — str, required (preferred)
  ``PAGINATION_CURSOR_SIGNING_SECRET``               — str, required fallback alias
  ``EXECUTION_PAGINATION_TOKEN_MAX_LENGTH``           — int, default per-module constant

All env vars have safe defaults and are validated fail-closed: unset or
unparseable values fall back to the safe default (e.g. TTL=3600, skew=300,
require_issued_at=True).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

PAGINATION_CURSOR_SECRET_MISSING = "PAGINATION_CURSOR_SECRET_MISSING"
PAGINATION_CURSOR_SECRET_WEAK = "PAGINATION_CURSOR_SECRET_WEAK"
PAGINATION_CURSOR_SIGNATURE_INVALID = "PAGINATION_CURSOR_SIGNATURE_INVALID"
PAGINATION_CURSOR_MIN_SECRET_BYTES = 32

_PRIMARY_CURSOR_SECRET_ENV = "PAGINATION_CURSOR_HMAC_SECRET"
_LEGACY_CURSOR_SECRET_ENV = "PAGINATION_CURSOR_SIGNING_SECRET"


class CursorSigningSecretMissing(ValueError):
    """Raised when cursor signing secret is not configured."""

    def __init__(self) -> None:
        """Initialize with the standard fail-closed reason code and guidance message."""
        super().__init__(
            f"{PAGINATION_CURSOR_SECRET_MISSING}: Cursor signing secret is not configured. "
            f"Set {_PRIMARY_CURSOR_SECRET_ENV} (preferred) or "
            f"{_LEGACY_CURSOR_SECRET_ENV} (legacy compatibility)."
        )
        self.reason_code = PAGINATION_CURSOR_SECRET_MISSING


class CursorSigningSecretWeak(ValueError):
    """Raised when cursor signing secret is configured but below minimum entropy length."""

    def __init__(self) -> None:
        """Initialize with a bounded reason code and non-sensitive guidance message."""
        super().__init__(
            f"{PAGINATION_CURSOR_SECRET_WEAK}: Cursor signing secret does not meet minimum "
            f"length requirements. Configure {_PRIMARY_CURSOR_SECRET_ENV} (preferred) or "
            f"{_LEGACY_CURSOR_SECRET_ENV} with at least {PAGINATION_CURSOR_MIN_SECRET_BYTES} "
            "bytes."
        )
        self.reason_code = PAGINATION_CURSOR_SECRET_WEAK


@dataclass(frozen=True)
class CursorSigningSecrets:
    """Normalized cursor-secret configuration state for fail-closed enforcement."""

    secret: str | None
    configured: bool
    valid: bool
    reason_code: str | None = None
    source_env_var: str | None = None

    @classmethod
    def from_env(cls) -> "CursorSigningSecrets":
        """Resolve cursor signing secret from environment with explicit migration behavior.

        Secret precedence:
        1. ``PAGINATION_CURSOR_HMAC_SECRET`` (preferred)
        2. ``PAGINATION_CURSOR_SIGNING_SECRET`` (legacy fallback alias)
        """
        secret_value, source_env_var = _resolve_env_secret_value()
        if secret_value is None:
            return cls(
                secret=None,
                configured=False,
                valid=False,
                reason_code=PAGINATION_CURSOR_SECRET_MISSING,
                source_env_var=None,
            )

        if len(secret_value.encode("utf-8")) < PAGINATION_CURSOR_MIN_SECRET_BYTES:
            return cls(
                secret=None,
                configured=True,
                valid=False,
                reason_code=PAGINATION_CURSOR_SECRET_WEAK,
                source_env_var=source_env_var,
            )

        return cls(
            secret=secret_value,
            configured=True,
            valid=True,
            reason_code=None,
            source_env_var=source_env_var,
        )

    def require_valid_secret(self) -> str:
        """Return the resolved secret or raise a bounded fail-closed error."""
        if self.valid and self.secret is not None:
            return self.secret
        if self.reason_code == PAGINATION_CURSOR_SECRET_WEAK:
            raise CursorSigningSecretWeak()
        raise CursorSigningSecretMissing()


def _resolve_env_secret_value() -> tuple[str | None, str | None]:
    """Resolve non-empty secret from preferred env var first, then legacy alias."""
    for env_name in (_PRIMARY_CURSOR_SECRET_ENV, _LEGACY_CURSOR_SECRET_ENV):
        raw_value = os.environ.get(env_name)
        if raw_value is None:
            continue
        stripped = raw_value.strip()
        if stripped:
            return stripped, env_name
    return None, None


def resolve_cursor_signing_secret(
    *,
    allow_unsigned: bool | None = None,
) -> str:
    """Resolve the cursor signing secret from environment configuration.

    ``allow_unsigned`` is retained for API compatibility and has no effect.
    Cursor signing is always fail-closed when the configured secret is
    missing or weak.
    """
    _ = allow_unsigned
    return CursorSigningSecrets.from_env().require_valid_secret()


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

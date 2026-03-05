"""Shared helpers for pagination cursor time-window metadata.

Cursor Guardrail Semantics
--------------------------
Pagination cursors (offset and keyset) embed time-window metadata and are
validated on decode with the following rules:

TTL
  Each cursor stamps signed ``issued_at_ms`` and ``ttl_ms`` (epoch
  milliseconds) on encode. On decode, missing TTL metadata is rejected
  fail-closed with ``PAGINATION_CURSOR_TTL_MISSING``; malformed values
  (non-integer, negative, overflow, zero ttl) are rejected with
  ``PAGINATION_CURSOR_TTL_INVALID``. Cursors older than ``ttl_ms`` are
  rejected with ``PAGINATION_CURSOR_EXPIRED``.

Clock-skew tolerance
  A cursor whose ``issued_at_ms`` is in the future beyond ``clock_skew_ms``
  (default 30000 ms / 30 s, configurable via
  ``PAGINATION_CURSOR_CLOCK_SKEW_MS``) is rejected with
  ``PAGINATION_CURSOR_CLOCK_SKEW``.

Query fingerprint binding
  When ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT`` is enabled (default True
  for keyset, False for offset) a SHA-256 ``query_fp`` is embedded in the
  cursor.  On decode the caller's recomputed fingerprint must match;
  mismatches are rejected with ``PAGINATION_CURSOR_QUERY_MISMATCH``.

Failure reason codes (bounded set)
  Every decode failure maps to exactly one of the following reason codes so
  that telemetry and error responses remain deterministic and bounded:

  - ``PAGINATION_CURSOR_TTL_MISSING``      — issued_at_ms/ttl_ms absent
  - ``PAGINATION_CURSOR_TTL_INVALID``      — issued_at_ms/ttl_ms malformed
  - ``PAGINATION_CURSOR_EXPIRED``          — TTL exceeded
  - ``PAGINATION_CURSOR_CLOCK_SKEW``       — issued_at_ms too far in the future
  - ``PAGINATION_CURSOR_QUERY_MISMATCH``   — query fingerprint mismatch
  - ``PAGINATION_CURSOR_SCOPE_MISSING``    — bound scope fingerprint absent
  - ``PAGINATION_CURSOR_SCOPE_MISMATCH``   — bound scope fingerprint mismatch
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
  ``PAGINATION_CURSOR_TTL_MS``                       — int, default 3600000
  ``PAGINATION_CURSOR_CLOCK_SKEW_MS``                — int, default 30000
  ``PAGINATION_CURSOR_BIND_QUERY_FINGERPRINT``       — bool, default True (keyset) / False (offset)
  ``PAGINATION_CURSOR_HMAC_SECRET``                  — str, required (preferred)
  ``PAGINATION_CURSOR_SIGNING_SECRET``               — str, required fallback alias
  ``EXECUTION_PAGINATION_TOKEN_MAX_LENGTH``           — int, default per-module constant

All env vars have safe defaults and are validated fail-closed: unset or
unparseable values fall back to the safe default (e.g. TTL=3600000, skew=30000).
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Any

PAGINATION_CURSOR_SECRET_MISSING = "PAGINATION_CURSOR_SECRET_MISSING"
PAGINATION_CURSOR_SECRET_WEAK = "PAGINATION_CURSOR_SECRET_WEAK"
PAGINATION_CURSOR_SIGNATURE_INVALID = "PAGINATION_CURSOR_SIGNATURE_INVALID"
PAGINATION_CURSOR_SCOPE_MISSING = "PAGINATION_CURSOR_SCOPE_MISSING"
PAGINATION_CURSOR_SCOPE_MISMATCH = "PAGINATION_CURSOR_SCOPE_MISMATCH"
PAGINATION_CURSOR_TTL_MISSING = "PAGINATION_CURSOR_TTL_MISSING"
PAGINATION_CURSOR_TTL_INVALID = "PAGINATION_CURSOR_TTL_INVALID"
PAGINATION_CURSOR_EXPIRED = "PAGINATION_CURSOR_EXPIRED"
PAGINATION_CURSOR_CLOCK_SKEW = "PAGINATION_CURSOR_CLOCK_SKEW"
PAGINATION_CURSOR_REPLAY_DETECTED = "PAGINATION_CURSOR_REPLAY_DETECTED"
PAGINATION_CURSOR_MIN_SECRET_BYTES = 32
PAGINATION_CURSOR_SCOPE_FINGERPRINT_HEX_LENGTH = 16
CURSOR_MAX_SIGNED_INT = 9_223_372_036_854_775_807
DEFAULT_CURSOR_TTL_MS = 3_600_000
DEFAULT_CURSOR_CLOCK_SKEW_MS = 30_000
MAX_CURSOR_TTL_MS = 86_400_000
MAX_CURSOR_CLOCK_SKEW_MS = 300_000
DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES = 10_000
MAX_CURSOR_REPLAY_CACHE_MAX_ENTRIES = 100_000
CURSOR_NONCE_MAX_LENGTH = 128

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


def cursor_now_epoch_milliseconds(*, now_epoch_milliseconds: int | None = None) -> int:
    """Return current unix epoch milliseconds from a single cursor clock source."""
    if now_epoch_milliseconds is None:
        return int(time.time()) * 1000
    return int(now_epoch_milliseconds)


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


def normalize_cursor_milliseconds(value: Any, *, allow_zero: bool = True) -> int | None:
    """Return bounded non-negative millisecond values for cursor metadata."""
    normalized = normalize_strict_int(value)
    if normalized is None:
        return None
    if normalized < 0:
        return None
    if not allow_zero and normalized <= 0:
        return None
    if normalized > CURSOR_MAX_SIGNED_INT:
        return None
    return normalized


def build_cursor_nonce() -> str:
    """Build a bounded opaque cursor nonce."""
    # 16 bytes -> 22 URL-safe characters; bounded and low-cardinality-safe.
    return secrets.token_urlsafe(16)


def normalize_cursor_nonce(value: Any) -> str | None:
    """Normalize nonces for replay guards while rejecting malformed values."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    if len(stripped) > CURSOR_NONCE_MAX_LENGTH:
        return None
    if any(ord(ch) < 33 or ord(ch) > 126 for ch in stripped):
        return None
    return stripped


def bounded_cursor_age_seconds(age_seconds: int, *, max_bound: int = 604_800) -> int:
    """Clamp cursor age to bounded telemetry-friendly integer ranges."""
    if age_seconds < 0:
        return 0
    return min(int(age_seconds), int(max_bound))


def normalize_cursor_scope_fingerprint(value: Any) -> str:
    """Normalize bounded scope fingerprints to lowercase fixed-length hex."""
    if not isinstance(value, str):
        return ""
    normalized = value.strip().lower()
    if len(normalized) != PAGINATION_CURSOR_SCOPE_FINGERPRINT_HEX_LENGTH:
        return ""
    if any(ch not in "0123456789abcdef" for ch in normalized):
        return ""
    return normalized


def build_cursor_scope_fingerprint(
    *,
    tenant_id: int | str | None,
    provider_name: str | None,
    provider_mode: str | None,
    tenant_enforcement_mode: str | None,
    pagination_mode: str | None,
    query_fingerprint: str | None,
) -> str:
    """Build deterministic bounded scope fingerprint used for cursor binding.

    The resulting value is a fixed-width prefix of SHA-256 over canonical JSON.
    Only normalized bounded fields are included to keep behavior deterministic.
    """
    normalized_tenant: int | str | None
    if tenant_id is None:
        normalized_tenant = None
    elif isinstance(tenant_id, bool):
        normalized_tenant = int(tenant_id)
    elif isinstance(tenant_id, int):
        normalized_tenant = int(tenant_id)
    else:
        normalized_tenant = str(tenant_id).strip() or None

    scope_struct = {
        "tenant_id": normalized_tenant,
        "provider_name": str(provider_name or "").strip().lower(),
        "provider_mode": str(provider_mode or "").strip().lower(),
        "tenant_enforcement_mode": str(tenant_enforcement_mode or "").strip().lower(),
        "pagination_mode": str(pagination_mode or "").strip().lower(),
        "query_fp": str(query_fingerprint or "").strip().lower(),
    }
    payload = json.dumps(scope_struct, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest_hex = hashlib.sha256(payload).hexdigest()
    return digest_hex[:PAGINATION_CURSOR_SCOPE_FINGERPRINT_HEX_LENGTH]


class _CursorReplayCache:
    """In-memory TTL+LRU cache for optional per-process cursor replay detection."""

    def __init__(self, *, max_entries: int = DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES) -> None:
        self._entries: OrderedDict[str, int] = OrderedDict()
        self._max_entries = max(1, int(max_entries))
        self._lock = Lock()

    def mark_once(
        self,
        nonce: str,
        *,
        now_ms: int,
        ttl_ms: int,
        max_entries: int = DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES,
    ) -> bool:
        """Return False when nonce is replayed within ttl; True on first-seen."""
        normalized_limit = min(max(1, int(max_entries)), MAX_CURSOR_REPLAY_CACHE_MAX_ENTRIES)
        expires_at_ms = now_ms + ttl_ms
        nonce_key = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        with self._lock:
            if normalized_limit != self._max_entries:
                self._max_entries = normalized_limit
            self._evict_expired_locked(now_ms)
            previous_expiry = self._entries.get(nonce_key)
            if previous_expiry is not None:
                if previous_expiry > now_ms:
                    return False
                self._entries.pop(nonce_key, None)
            self._entries[nonce_key] = expires_at_ms
            self._entries.move_to_end(nonce_key)
            self._evict_lru_locked()
            return True

    def _evict_expired_locked(self, now_ms: int) -> None:
        expired_keys = [key for key, expires_at in self._entries.items() if expires_at <= now_ms]
        for key in expired_keys:
            self._entries.pop(key, None)

    def _evict_lru_locked(self) -> None:
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)


_CURSOR_REPLAY_CACHE = _CursorReplayCache()


def register_cursor_nonce_once(
    *,
    nonce: str,
    now_ms: int,
    ttl_ms: int,
    max_entries: int = DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES,
) -> bool | None:
    """Try registering nonce in replay cache.

    Returns:
      - True: nonce accepted (first use)
      - False: nonce replay detected
      - None: replay cache unavailable -> caller should use guard-disabled semantics
    """
    try:
        return _CURSOR_REPLAY_CACHE.mark_once(
            nonce,
            now_ms=now_ms,
            ttl_ms=ttl_ms,
            max_entries=max_entries,
        )
    except Exception:
        return None

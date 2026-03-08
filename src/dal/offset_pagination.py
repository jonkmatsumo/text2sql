"""Deterministic offset-pagination tokens for provider-agnostic paging."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from dal.execution_budget import (
    PAGINATION_BUDGET_SNAPSHOT_INVALID,
    ExecutionBudget,
    ExecutionBudgetSnapshotError,
    budget_snapshot_fingerprint,
)
from dal.pagination_cursor import (
    DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES,
    DEFAULT_CURSOR_TTL_MS,
    PAGINATION_CURSOR_CLOCK_SKEW,
    PAGINATION_CURSOR_EXPIRED,
    PAGINATION_CURSOR_KEYRING_INVALID,
    PAGINATION_CURSOR_KID_MISSING,
    PAGINATION_CURSOR_KID_UNKNOWN,
    PAGINATION_CURSOR_MIGRATION_UNSAFE,
    PAGINATION_CURSOR_REPLAY_DETECTED,
    PAGINATION_CURSOR_SCOPE_MISMATCH,
    PAGINATION_CURSOR_SCOPE_MISSING,
    PAGINATION_CURSOR_SECRET_MISSING,
    PAGINATION_CURSOR_SECRET_WEAK,
    PAGINATION_CURSOR_SIGNATURE_INVALID,
    PAGINATION_CURSOR_TTL_INVALID,
    PAGINATION_CURSOR_TTL_MISSING,
    CursorMigrationError,
    CursorMigrationRegistry,
    CursorSigningKeyring,
    bounded_cursor_age_seconds,
    build_cursor_envelope,
    build_cursor_nonce,
    cursor_now_epoch_milliseconds,
    normalize_cursor_kid,
    normalize_cursor_milliseconds,
    normalize_cursor_nonce,
    normalize_cursor_scope_fingerprint,
    normalize_optional_int,
    register_cursor_nonce_once,
)

PAGINATION_CURSOR_QUERY_MISMATCH = "PAGINATION_CURSOR_QUERY_MISMATCH"


class OffsetPaginationTokenError(ValueError):
    """Raised when pagination token validation fails."""

    def __init__(self, *, reason_code: str, message: str) -> None:
        """Attach a deterministic reason code to token validation errors."""
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True)
class OffsetPaginationToken:
    """Parsed deterministic pagination token payload."""

    offset: int
    limit: int
    fingerprint: str
    issued_at_ms: int | None = None
    ttl_ms: int | None = None
    issued_at: int | None = None
    max_age_s: int | None = None
    legacy_issued_at_accepted: bool = False
    nonce: str | None = None
    kid: str | None = None
    query_fingerprint: str | None = None
    budget_snapshot: dict[str, Any] | None = None


@dataclass(frozen=True)
class OffsetCursorMigrationResult:
    """Normalized offset payload plus bounded migration telemetry fields."""

    payload: dict[str, Any]
    original_version: int
    current_version: int
    migration_attempted: bool
    migration_outcome: str


_OFFSET_CURSOR_KIND = "offset"
_OFFSET_CURSOR_CURRENT_VERSION = 1
_OFFSET_CURSOR_DEFAULT_KID = "legacy"


def _extract_first_present(payload: dict[str, Any], *keys: str) -> Any:
    """Return the first payload value found for any candidate key."""
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _derived_legacy_offset_nonce(payload: dict[str, Any]) -> str:
    """Derive deterministic nonce for legacy payloads that predate nonce support."""
    nonce_seed = {
        "o": payload.get("o"),
        "l": payload.get("l"),
        "f": payload.get("f"),
        "issued_at_ms": payload.get("issued_at_ms"),
        "ttl_ms": payload.get("ttl_ms"),
        "query_fp": payload.get("query_fp"),
        "scope_fp": payload.get("scope_fp"),
    }
    seed_bytes = json.dumps(nonce_seed, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(seed_bytes).hexdigest()[:32]
    return f"legacy-{digest}"


def _migrate_offset_payload_v0_to_v1(payload: dict[str, Any]) -> dict[str, Any]:
    """Migrate legacy offset payloads to explicit v1 envelope fields."""
    offset_raw = _extract_first_present(payload, "o", "offset")
    limit_raw = _extract_first_present(payload, "l", "limit")
    fingerprint_raw = _extract_first_present(payload, "f", "fingerprint")
    offset = normalize_optional_int(offset_raw)
    limit = normalize_optional_int(limit_raw)
    fingerprint = str(fingerprint_raw).strip() if fingerprint_raw is not None else ""
    if offset is None or limit is None or not fingerprint:
        raise CursorMigrationError(
            reason_code=PAGINATION_CURSOR_MIGRATION_UNSAFE,
            message="Legacy offset cursor is missing required pagination fields.",
        )

    issued_at_ms = normalize_cursor_milliseconds(_extract_first_present(payload, "issued_at_ms"))
    if issued_at_ms is None:
        issued_at = normalize_optional_int(_extract_first_present(payload, "issued_at"))
        if issued_at is not None:
            issued_at_ms = normalize_cursor_milliseconds(issued_at * 1000)
    ttl_ms = normalize_cursor_milliseconds(
        _extract_first_present(payload, "ttl_ms"), allow_zero=False
    )
    if ttl_ms is None:
        max_age_s = normalize_optional_int(_extract_first_present(payload, "max_age_s"))
        if max_age_s is not None:
            ttl_ms = normalize_cursor_milliseconds(max_age_s * 1000, allow_zero=False)
    if issued_at_ms is None or ttl_ms is None:
        raise CursorMigrationError(
            reason_code=PAGINATION_CURSOR_MIGRATION_UNSAFE,
            message="Legacy offset cursor cannot be migrated safely without ttl metadata.",
        )

    nonce = normalize_cursor_nonce(_extract_first_present(payload, "nonce"))
    if nonce is None:
        nonce = _derived_legacy_offset_nonce(
            {
                "o": int(offset),
                "l": int(limit),
                "f": fingerprint,
                "issued_at_ms": int(issued_at_ms),
                "ttl_ms": int(ttl_ms),
                "query_fp": _extract_first_present(payload, "query_fp"),
                "scope_fp": _extract_first_present(payload, "scope_fp"),
            }
        )
    query_fp_raw = _extract_first_present(payload, "query_fp")
    query_fp = str(query_fp_raw).strip() if isinstance(query_fp_raw, str) else ""
    scope_fp = normalize_cursor_scope_fingerprint(_extract_first_present(payload, "scope_fp"))
    kid = normalize_cursor_kid(_extract_first_present(payload, "kid"))

    migrated_payload: dict[str, Any] = {
        "cursor_version": _OFFSET_CURSOR_CURRENT_VERSION,
        "cursor_kind": _OFFSET_CURSOR_KIND,
        "v": _OFFSET_CURSOR_CURRENT_VERSION,
        "o": int(offset),
        "l": int(limit),
        "f": fingerprint,
        "issued_at_ms": int(issued_at_ms),
        "ttl_ms": int(ttl_ms),
        "issued_at": int(issued_at_ms) // 1000,
        "max_age_s": max(1, int(ttl_ms) // 1000),
        "nonce": nonce,
    }
    if query_fp:
        migrated_payload["query_fp"] = query_fp
    if scope_fp:
        migrated_payload["scope_fp"] = scope_fp
    if kid:
        migrated_payload["kid"] = kid
    if "budget_snapshot" in payload:
        migrated_payload["budget_snapshot"] = payload.get("budget_snapshot")
    if "budget_fp" in payload:
        migrated_payload["budget_fp"] = payload.get("budget_fp")
    return migrated_payload


_OFFSET_CURSOR_MIGRATION_REGISTRY = CursorMigrationRegistry(
    current_versions={_OFFSET_CURSOR_KIND: _OFFSET_CURSOR_CURRENT_VERSION}
)
_OFFSET_CURSOR_MIGRATION_REGISTRY.register(
    cursor_kind=_OFFSET_CURSOR_KIND,
    from_version=0,
    to_version=1,
    migration=_migrate_offset_payload_v0_to_v1,
)


def _migrate_offset_payload(raw_payload: dict[str, Any]) -> OffsetCursorMigrationResult:
    """Normalize raw payload into current offset cursor payload contract."""
    current_version = _OFFSET_CURSOR_MIGRATION_REGISTRY.current_version_for_kind(
        _OFFSET_CURSOR_KIND
    )
    envelope = build_cursor_envelope(
        raw_payload=raw_payload,
        cursor_kind=_OFFSET_CURSOR_KIND,
        allow_legacy_v0=True,
    )
    original_version = int(envelope.cursor_version)
    migration_attempted = original_version < current_version
    migrated = _OFFSET_CURSOR_MIGRATION_REGISTRY.migrate(envelope)
    return OffsetCursorMigrationResult(
        payload=migrated.payload,
        original_version=original_version,
        current_version=current_version,
        migration_attempted=migration_attempted,
        migration_outcome="migrated" if migration_attempted else "not_needed",
    )


def build_query_fingerprint(
    *,
    sql: str,
    params: list[Any] | None,
    tenant_id: int | None,
    provider: str,
    max_rows: int,
    max_bytes: int,
    max_execution_ms: int,
    order_signature: str | None = None,
    backend_signature: str | None = None,
) -> str:
    """Build a stable fingerprint binding pagination tokens to execution context."""
    sql_normalized = " ".join((sql or "").strip().split())
    params_json = json.dumps(params or [], default=str, separators=(",", ":"), sort_keys=True)
    payload = {
        "sql": sql_normalized,
        "params_hash": hashlib.sha256(params_json.encode("utf-8")).hexdigest(),
        "tenant_id": int(tenant_id) if tenant_id is not None else None,
        "provider": (provider or "").strip().lower(),
        "max_rows": int(max_rows),
        "max_bytes": int(max_bytes),
        "max_execution_ms": int(max_execution_ms),
    }
    if order_signature is not None:
        payload["order_signature"] = " ".join(order_signature.strip().split())
    if backend_signature is not None:
        payload["backend_signature"] = str(backend_signature).strip()
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_cursor_query_fingerprint(
    *,
    sql: str,
    provider: str,
    pagination_mode: str,
    order_signature: str | None = None,
) -> str:
    """Build a stable fingerprint for strict cursor replay protection checks."""
    sql_normalized = " ".join((sql or "").strip().split())
    normalized_mode = (pagination_mode or "").strip().lower() or "offset"
    payload = {
        "sql": sql_normalized,
        "provider": (provider or "").strip().lower(),
        "pagination_mode": normalized_mode,
    }
    if order_signature is not None:
        payload["order_signature"] = " ".join(order_signature.strip().split())
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def encode_offset_pagination_token(
    *,
    offset: int,
    limit: int,
    fingerprint: str,
    signing_keyring: CursorSigningKeyring | None = None,
    secret: str | None = None,
    issued_at_ms: int | None = None,
    ttl_ms: int | None = None,
    nonce: str | None = None,
    issued_at: int | None = None,
    max_age_s: int | None = None,
    kid: str | None = None,
    now_epoch_milliseconds: int | None = None,
    now_epoch_seconds: int | None = None,
    query_fp: str | None = None,
    scope_fp: str | None = None,
    budget_snapshot: dict[str, Any] | None = None,
) -> str:
    """Encode a deterministic opaque pagination token."""
    signing_secret = (secret or "").strip()
    active_kid: str | None = None
    if signing_keyring is not None:
        try:
            active_kid, signing_secret = signing_keyring.require_active_signing_key()
        except Exception as exc:
            reason_code = getattr(exc, "reason_code", PAGINATION_CURSOR_KEYRING_INVALID)
            raise OffsetPaginationTokenError(
                reason_code=reason_code,
                message="Invalid pagination token metadata.",
            ) from exc

    if now_epoch_milliseconds is None and now_epoch_seconds is not None:
        now_epoch_milliseconds = int(now_epoch_seconds) * 1000
    normalized_issued_at_ms = normalize_cursor_milliseconds(issued_at_ms)
    if normalized_issued_at_ms is None and issued_at is not None:
        normalized_issued_at = normalize_optional_int(issued_at)
        if normalized_issued_at is not None:
            normalized_issued_at_ms = normalize_cursor_milliseconds(normalized_issued_at * 1000)
    if normalized_issued_at_ms is None:
        normalized_issued_at_ms = cursor_now_epoch_milliseconds(
            now_epoch_milliseconds=now_epoch_milliseconds
        )

    normalized_ttl_ms = normalize_cursor_milliseconds(ttl_ms, allow_zero=False)
    if normalized_ttl_ms is None:
        normalized_max_age = normalize_optional_int(max_age_s)
        if normalized_max_age is not None:
            normalized_ttl_ms = normalize_cursor_milliseconds(
                normalized_max_age * 1000, allow_zero=False
            )
    if normalized_ttl_ms is None:
        normalized_ttl_ms = DEFAULT_CURSOR_TTL_MS

    payload: dict[str, Any] = {
        "cursor_version": _OFFSET_CURSOR_CURRENT_VERSION,
        "cursor_kind": _OFFSET_CURSOR_KIND,
        "v": 1,
        "o": int(offset),
        "l": int(limit),
        "f": str(fingerprint),
        "issued_at_ms": int(normalized_issued_at_ms),
        "ttl_ms": int(normalized_ttl_ms),
        # Backwards-compatible aliases while consumers migrate to *_ms fields.
        "issued_at": int(normalized_issued_at_ms) // 1000,
        "max_age_s": max(1, int(normalized_ttl_ms) // 1000),
    }
    normalized_kid = normalize_cursor_kid(
        active_kid
        if active_kid is not None
        else (kid if kid is not None else _OFFSET_CURSOR_DEFAULT_KID)
    )
    if normalized_kid is None:
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_KID_MISSING,
            message="Invalid pagination token metadata.",
        )
    payload["kid"] = normalized_kid
    normalized_nonce = normalize_cursor_nonce(nonce) or build_cursor_nonce()
    payload["nonce"] = normalized_nonce
    normalized_query_fp = str(query_fp).strip() if isinstance(query_fp, str) else ""
    if normalized_query_fp:
        payload["query_fp"] = normalized_query_fp
    normalized_scope_fp = normalize_cursor_scope_fingerprint(scope_fp)
    if normalized_scope_fp:
        payload["scope_fp"] = normalized_scope_fp
    if budget_snapshot is not None:
        normalized_budget_snapshot = ExecutionBudget.from_snapshot(budget_snapshot).to_snapshot()
        payload["budget_snapshot"] = normalized_budget_snapshot
        payload["budget_fp"] = budget_snapshot_fingerprint(normalized_budget_snapshot)
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    wrapper: dict[str, Any] = {"p": payload}
    if signing_secret:
        signature = hmac.new(
            signing_secret.encode("utf-8"), payload_bytes, digestmod=hashlib.sha256
        ).hexdigest()
        wrapper["s"] = signature
    encoded = base64.urlsafe_b64encode(
        json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")
    return encoded.rstrip("=")


def decode_offset_pagination_token(
    *,
    token: str,
    expected_fingerprint: str,
    max_length: int,
    signing_keyring: CursorSigningKeyring | None = None,
    secret: str | None = None,
    require_issued_at: bool = True,
    decode_metadata: dict[str, Any] | None = None,
    max_age_seconds: int | None = 3600,
    clock_skew_seconds: int = 300,
    clock_skew_ms: int | None = None,
    replay_guard_enabled: bool = False,
    replay_cache_max_entries: int = DEFAULT_CURSOR_REPLAY_CACHE_MAX_ENTRIES,
    now_epoch_seconds: int | None = None,
    now_epoch_milliseconds: int | None = None,
    expected_query_fp: str | None = None,
    expected_scope_fp: str | None = None,
) -> OffsetPaginationToken:
    """Decode and validate an offset pagination token."""
    _ = require_issued_at  # Legacy compatibility only; ttl metadata is always required.
    _ = max_age_seconds  # Legacy compatibility only; signed ttl_ms is authoritative.
    normalized_token = (token or "").strip()
    if not normalized_token:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_invalid",
            message="Invalid pagination token.",
        )
    if len(normalized_token) > max_length:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_too_long",
            message="Pagination token exceeds maximum length.",
        )
    padded = normalized_token + "=" * (-len(normalized_token) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        raw_wrapper = json.loads(decoded)
    except Exception as exc:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_malformed",
            message="Malformed pagination token.",
        ) from exc

    if not isinstance(raw_wrapper, dict):
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_malformed",
            message="Malformed pagination token payload.",
        )

    payload = raw_wrapper.get("p")
    if not isinstance(payload, dict):
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_malformed",
            message="Malformed pagination token payload.",
        )
    payload_kid = normalize_cursor_kid(payload.get("kid"))
    if isinstance(decode_metadata, dict):
        decode_metadata["kid_present"] = payload_kid is not None
    if payload_kid is None:
        if isinstance(decode_metadata, dict):
            decode_metadata["validation_outcome"] = "INVALID"
            decode_metadata["rotation_verification_path"] = "error"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_KID_MISSING,
            message="Invalid pagination token metadata.",
        )

    secret_value = (secret or "").strip()
    if signing_keyring is not None:
        try:
            secret_value = signing_keyring.resolve_verifier_secret(payload_kid)
            if isinstance(decode_metadata, dict):
                kid_active_match = bool(payload_kid == signing_keyring.active_kid)
                decode_metadata["kid_active_match"] = kid_active_match
                decode_metadata["rotation_verification_path"] = (
                    "active" if kid_active_match else "secondary"
                )
        except Exception as exc:
            reason_code = getattr(exc, "reason_code", PAGINATION_CURSOR_KEYRING_INVALID)
            if reason_code not in {
                PAGINATION_CURSOR_KID_MISSING,
                PAGINATION_CURSOR_KID_UNKNOWN,
                PAGINATION_CURSOR_SECRET_MISSING,
                PAGINATION_CURSOR_SECRET_WEAK,
            }:
                reason_code = PAGINATION_CURSOR_KEYRING_INVALID
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "INVALID"
                decode_metadata["rotation_verification_path"] = "error"
            raise OffsetPaginationTokenError(
                reason_code=reason_code,
                message="Invalid pagination token metadata.",
            ) from exc
    signature = raw_wrapper.get("s")
    if secret_value:
        if not isinstance(signature, str) or not signature:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "SIGNATURE_INVALID"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_SIGNATURE_INVALID,
                message="Invalid pagination token signature.",
            )
        payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        expected_signature = hmac.new(
            secret_value.encode("utf-8"), payload_bytes, digestmod=hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "SIGNATURE_INVALID"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_SIGNATURE_INVALID,
                message="Invalid pagination token signature.",
            )

    try:
        migration_result = _migrate_offset_payload(payload)
    except CursorMigrationError as exc:
        if isinstance(decode_metadata, dict):
            raw_original_version = normalize_optional_int(payload.get("cursor_version"))
            if raw_original_version is None and "cursor_version" not in payload:
                raw_original_version = 0
            decode_metadata["migration_attempted"] = bool(
                raw_original_version is not None
                and raw_original_version < _OFFSET_CURSOR_CURRENT_VERSION
            )
            decode_metadata["migration_outcome"] = "rejected"
            if raw_original_version is not None:
                decode_metadata["original_version"] = int(raw_original_version)
            decode_metadata["current_version"] = _OFFSET_CURSOR_CURRENT_VERSION
            decode_metadata["validation_outcome"] = "INVALID"
        raise OffsetPaginationTokenError(
            reason_code=exc.reason_code,
            message="Invalid pagination token metadata.",
        ) from exc
    payload = migration_result.payload
    payload_kid = normalize_cursor_kid(payload.get("kid"))
    if payload_kid is None:
        if isinstance(decode_metadata, dict):
            decode_metadata["validation_outcome"] = "INVALID"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_KID_MISSING,
            message="Invalid pagination token metadata.",
        )
    if isinstance(decode_metadata, dict):
        decode_metadata["migration_attempted"] = migration_result.migration_attempted
        decode_metadata["migration_outcome"] = migration_result.migration_outcome
        decode_metadata["original_version"] = migration_result.original_version
        decode_metadata["current_version"] = migration_result.current_version
    try:
        version = int(payload.get("v"))
        offset = int(payload.get("o"))
        limit = int(payload.get("l"))
        fingerprint = str(payload.get("f"))
    except Exception as exc:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_malformed",
            message="Malformed pagination token payload.",
        ) from exc

    if version != 1 or offset < 0 or limit <= 0 or not fingerprint:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_malformed",
            message="Malformed pagination token payload.",
        )

    issued_at_ms_payload = payload.get("issued_at_ms")
    ttl_ms_payload = payload.get("ttl_ms")
    if isinstance(decode_metadata, dict):
        decode_metadata["issued_at_present"] = issued_at_ms_payload is not None
        decode_metadata["scope_bound"] = False
        decode_metadata["scope_mismatch"] = False
        decode_metadata["replay_guard_enabled"] = bool(replay_guard_enabled)
    if issued_at_ms_payload is None or ttl_ms_payload is None:
        if isinstance(decode_metadata, dict):
            decode_metadata["validation_outcome"] = "INVALID"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_TTL_MISSING,
            message="Invalid pagination token metadata.",
        )

    issued_at_ms = normalize_cursor_milliseconds(issued_at_ms_payload)
    ttl_ms_value = normalize_cursor_milliseconds(ttl_ms_payload, allow_zero=False)
    if issued_at_ms is None or ttl_ms_value is None:
        if isinstance(decode_metadata, dict):
            decode_metadata["validation_outcome"] = "INVALID"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_TTL_INVALID,
            message="Invalid pagination token metadata.",
        )
    query_fp = payload.get("query_fp")
    query_fingerprint = str(query_fp).strip() if isinstance(query_fp, str) else None
    budget_snapshot = payload.get("budget_snapshot")
    budget_fp = payload.get("budget_fp")
    normalized_budget_snapshot: dict[str, Any] | None = None
    if budget_snapshot is not None or budget_fp is not None:
        try:
            normalized_budget_snapshot = ExecutionBudget.from_snapshot(
                budget_snapshot
            ).to_snapshot()
            expected_budget_fp = budget_snapshot_fingerprint(normalized_budget_snapshot)
        except ExecutionBudgetSnapshotError as exc:
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_BUDGET_SNAPSHOT_INVALID,
                message=str(exc),
            ) from exc
        if not isinstance(budget_fp, str) or budget_fp != expected_budget_fp:
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_BUDGET_SNAPSHOT_INVALID,
                message="Invalid pagination budget snapshot fingerprint.",
            )
    if now_epoch_milliseconds is None and now_epoch_seconds is not None:
        now_epoch_milliseconds = int(now_epoch_seconds) * 1000
    now_ms = cursor_now_epoch_milliseconds(now_epoch_milliseconds=now_epoch_milliseconds)
    if clock_skew_ms is None:
        normalized_clock_skew_ms = max(0, int(clock_skew_seconds)) * 1000
    else:
        normalized_clock_skew_ms = max(0, int(clock_skew_ms))
    if issued_at_ms > now_ms + normalized_clock_skew_ms:
        if isinstance(decode_metadata, dict):
            decode_metadata["age_s"] = 0
            decode_metadata["skew_detected"] = True
            decode_metadata["validation_outcome"] = "SKEW"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_CLOCK_SKEW,
            message="Invalid pagination token metadata.",
        )
    age_ms = now_ms - issued_at_ms
    if isinstance(decode_metadata, dict):
        decode_metadata["age_s"] = bounded_cursor_age_seconds(age_ms // 1000)
    if age_ms > int(ttl_ms_value):
        if isinstance(decode_metadata, dict):
            decode_metadata["expired"] = True
            decode_metadata["validation_outcome"] = "EXPIRED"
        raise OffsetPaginationTokenError(
            reason_code=PAGINATION_CURSOR_EXPIRED,
            message="Pagination token has expired.",
        )
    if isinstance(decode_metadata, dict):
        decode_metadata["validation_outcome"] = "OK"
    expected_query_fingerprint = (
        str(expected_query_fp).strip() if isinstance(expected_query_fp, str) else None
    )
    if expected_query_fingerprint:
        if query_fingerprint != expected_query_fingerprint:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "QUERY_MISMATCH"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_QUERY_MISMATCH,
                message="Pagination token does not match the current query fingerprint.",
            )
    expected_scope_fingerprint = normalize_cursor_scope_fingerprint(expected_scope_fp)
    if expected_scope_fingerprint:
        if isinstance(decode_metadata, dict):
            decode_metadata["scope_bound"] = True
        payload_scope_fingerprint = normalize_cursor_scope_fingerprint(payload.get("scope_fp"))
        if not payload_scope_fingerprint:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "SCOPE_MISSING"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_SCOPE_MISSING,
                message="Pagination token is missing required scope binding.",
            )
        if payload_scope_fingerprint != expected_scope_fingerprint:
            if isinstance(decode_metadata, dict):
                decode_metadata["scope_mismatch"] = True
                decode_metadata["validation_outcome"] = "SCOPE_MISMATCH"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_SCOPE_MISMATCH,
                message="Pagination token does not match the current request scope.",
            )
    if fingerprint != expected_fingerprint:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_fingerprint_mismatch",
            message="Pagination token does not match the current query.",
        )
    nonce = normalize_cursor_nonce(payload.get("nonce"))
    if replay_guard_enabled:
        if nonce is None:
            if isinstance(decode_metadata, dict):
                decode_metadata["replay_detected"] = True
                decode_metadata["validation_outcome"] = "REPLAY"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_REPLAY_DETECTED,
                message="Pagination token replay rejected.",
            )
        replay_cache_result = register_cursor_nonce_once(
            nonce=nonce,
            now_ms=now_ms,
            ttl_ms=ttl_ms_value,
            max_entries=replay_cache_max_entries,
        )
        if replay_cache_result is False:
            if isinstance(decode_metadata, dict):
                decode_metadata["replay_detected"] = True
                decode_metadata["validation_outcome"] = "REPLAY"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_REPLAY_DETECTED,
                message="Pagination token replay rejected.",
            )

    return OffsetPaginationToken(
        offset=offset,
        limit=limit,
        fingerprint=fingerprint,
        issued_at_ms=issued_at_ms,
        ttl_ms=ttl_ms_value,
        issued_at=issued_at_ms // 1000,
        max_age_s=ttl_ms_value // 1000,
        legacy_issued_at_accepted=False,
        nonce=nonce,
        kid=payload_kid,
        query_fingerprint=query_fingerprint,
        budget_snapshot=normalized_budget_snapshot,
    )

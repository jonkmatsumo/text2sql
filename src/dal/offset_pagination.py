"""Deterministic offset-pagination tokens for provider-agnostic paging."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from dal.pagination_cursor import (
    bounded_cursor_age_seconds,
    cursor_now_epoch_seconds,
    normalize_optional_int,
    normalize_strict_int,
)

PAGINATION_CURSOR_EXPIRED = "PAGINATION_CURSOR_EXPIRED"
PAGINATION_CURSOR_ISSUED_AT_INVALID = "PAGINATION_CURSOR_ISSUED_AT_INVALID"
PAGINATION_CURSOR_CLOCK_SKEW = "PAGINATION_CURSOR_CLOCK_SKEW"
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
    issued_at: int | None = None
    max_age_s: int | None = None
    legacy_issued_at_accepted: bool = False
    query_fingerprint: str | None = None


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
    secret: str | None = None,
    issued_at: int | None = None,
    max_age_s: int | None = None,
    now_epoch_seconds: int | None = None,
    query_fp: str | None = None,
) -> str:
    """Encode a deterministic opaque pagination token."""
    payload: dict[str, Any] = {
        "v": 1,
        "o": int(offset),
        "l": int(limit),
        "f": str(fingerprint),
        "issued_at": cursor_now_epoch_seconds(
            now_epoch_seconds=issued_at if issued_at is not None else now_epoch_seconds
        ),
    }
    normalized_max_age = normalize_optional_int(max_age_s)
    if normalized_max_age is not None:
        payload["max_age_s"] = normalized_max_age
    normalized_query_fp = str(query_fp).strip() if isinstance(query_fp, str) else ""
    if normalized_query_fp:
        payload["query_fp"] = normalized_query_fp
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    wrapper: dict[str, Any] = {"p": payload}
    secret_value = (secret or "").strip()
    if secret_value:
        signature = hmac.new(
            secret_value.encode("utf-8"), payload_bytes, digestmod=hashlib.sha256
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
    secret: str | None = None,
    require_issued_at: bool = True,
    decode_metadata: dict[str, Any] | None = None,
    max_age_seconds: int | None = 3600,
    clock_skew_seconds: int = 300,
    now_epoch_seconds: int | None = None,
    expected_query_fp: str | None = None,
) -> OffsetPaginationToken:
    """Decode and validate an offset pagination token."""
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
    secret_value = (secret or "").strip()
    signature = raw_wrapper.get("s")
    if secret_value:
        if not isinstance(signature, str) or not signature:
            raise OffsetPaginationTokenError(
                reason_code="execution_pagination_page_token_signature_invalid",
                message="Invalid pagination token signature.",
            )
        payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        expected_signature = hmac.new(
            secret_value.encode("utf-8"), payload_bytes, digestmod=hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected_signature):
            raise OffsetPaginationTokenError(
                reason_code="execution_pagination_page_token_signature_invalid",
                message="Invalid pagination token signature.",
            )

    issued_at = normalize_strict_int(payload.get("issued_at"))
    legacy_issued_at_accepted = False
    if isinstance(decode_metadata, dict):
        decode_metadata["expired"] = False
        decode_metadata["skew_detected"] = False
        decode_metadata["issued_at_present"] = issued_at is not None
    if issued_at is None:
        if require_issued_at:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "INVALID"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_ISSUED_AT_INVALID,
                message="Invalid pagination token: issued_at is required.",
            )
        legacy_issued_at_accepted = True
        if isinstance(decode_metadata, dict):
            decode_metadata["legacy_issued_at_accepted"] = True
            decode_metadata["validation_outcome"] = "LEGACY_ACCEPTED"
    max_age_payload = payload.get("max_age_s")
    max_age_s = None
    if max_age_payload is not None:
        max_age_s = normalize_strict_int(max_age_payload)
        if max_age_s is None:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "INVALID"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_ISSUED_AT_INVALID,
                message="Invalid pagination token: max_age_s must be an integer.",
            )
    query_fp = payload.get("query_fp")
    query_fingerprint = str(query_fp).strip() if isinstance(query_fp, str) else None
    if issued_at is not None:
        effective_max_age_s = max_age_s
        if effective_max_age_s is None:
            effective_max_age_s = normalize_optional_int(max_age_seconds)
        if effective_max_age_s is None or effective_max_age_s <= 0:
            if isinstance(decode_metadata, dict):
                decode_metadata["validation_outcome"] = "INVALID"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_ISSUED_AT_INVALID,
                message="Invalid pagination token: max_age_s is required.",
            )
        now_epoch = cursor_now_epoch_seconds(now_epoch_seconds=now_epoch_seconds)
        skew_seconds = max(0, int(clock_skew_seconds))
        if issued_at > now_epoch + skew_seconds:
            if isinstance(decode_metadata, dict):
                decode_metadata["age_s"] = 0
                decode_metadata["skew_detected"] = True
                decode_metadata["validation_outcome"] = "SKEW"
            raise OffsetPaginationTokenError(
                reason_code=PAGINATION_CURSOR_CLOCK_SKEW,
                message="Invalid pagination token: issued_at is in the future.",
            )
        age_seconds = now_epoch - issued_at
        if isinstance(decode_metadata, dict):
            decode_metadata["age_s"] = bounded_cursor_age_seconds(age_seconds)
        if age_seconds > int(effective_max_age_s):
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
    if fingerprint != expected_fingerprint:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_fingerprint_mismatch",
            message="Pagination token does not match the current query.",
        )

    return OffsetPaginationToken(
        offset=offset,
        limit=limit,
        fingerprint=fingerprint,
        issued_at=issued_at,
        max_age_s=max_age_s,
        legacy_issued_at_accepted=legacy_issued_at_accepted,
        query_fingerprint=query_fingerprint,
    )

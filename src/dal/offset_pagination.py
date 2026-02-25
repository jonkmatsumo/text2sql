"""Deterministic offset-pagination tokens for provider-agnostic paging."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any


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
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def encode_offset_pagination_token(
    *,
    offset: int,
    limit: int,
    fingerprint: str,
    secret: str | None = None,
) -> str:
    """Encode a deterministic opaque pagination token."""
    payload = {"v": 1, "o": int(offset), "l": int(limit), "f": str(fingerprint)}
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

    if fingerprint != expected_fingerprint:
        raise OffsetPaginationTokenError(
            reason_code="execution_pagination_page_token_fingerprint_mismatch",
            message="Pagination token does not match the current query.",
        )

    return OffsetPaginationToken(offset=offset, limit=limit, fingerprint=fingerprint)

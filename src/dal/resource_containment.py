"""Execution-boundary result containment helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from common.constants.reason_codes import PayloadTruncationReason


class ResourceContainmentPolicyError(RuntimeError):
    """Raised when requested containment enforcement is unsupported by provider capabilities."""

    def __init__(self, *, reason_code: str, required_capability: str, message: str) -> None:
        """Capture deterministic reason code and capability name for fail-closed errors."""
        super().__init__(message)
        self.reason_code = reason_code
        self.required_capability = required_capability


@dataclass(frozen=True)
class RowContainmentResult:
    """Result of hard row-limit enforcement."""

    rows: list[dict[str, Any]]
    partial: bool
    partial_reason: str | None
    items_returned: int


@dataclass(frozen=True)
class ByteContainmentResult:
    """Result of hard byte-limit enforcement."""

    rows: list[dict[str, Any]]
    partial: bool
    partial_reason: str | None
    items_returned: int
    bytes_returned: int


def validate_resource_capabilities(
    *,
    provider: str,
    enforce_row_limit: bool,
    enforce_byte_limit: bool,
    enforce_timeout: bool,
    supports_row_cap: bool,
    supports_byte_cap: bool,
    supports_timeout: bool,
) -> None:
    """Fail closed when configured enforcement lacks provider capability support."""
    normalized_provider = (provider or "unspecified").strip().lower() or "unspecified"
    if enforce_row_limit and not supports_row_cap:
        raise ResourceContainmentPolicyError(
            reason_code="execution_resource_row_cap_unsupported_provider",
            required_capability="row_cap",
            message=(
                "Execution row-cap enforcement is not supported for provider "
                f"'{normalized_provider}'."
            ),
        )
    if enforce_byte_limit and not supports_byte_cap:
        raise ResourceContainmentPolicyError(
            reason_code="execution_resource_byte_cap_unsupported_provider",
            required_capability="byte_cap",
            message=(
                "Execution byte-cap enforcement is not supported for provider "
                f"'{normalized_provider}'."
            ),
        )
    if enforce_timeout and not supports_timeout:
        raise ResourceContainmentPolicyError(
            reason_code="execution_resource_timeout_unsupported_provider",
            required_capability="timeout",
            message=(
                "Execution timeout enforcement is not supported for provider "
                f"'{normalized_provider}'."
            ),
        )


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


def _json_size_bytes(payload: Any) -> int:
    try:
        return len(json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return 0


def enforce_byte_limit(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_bytes: int,
    enforce: bool,
    envelope_overhead: Mapping[str, Any] | None = None,
) -> ByteContainmentResult:
    """Apply a deterministic byte cap without emitting partial rows."""
    bounded_rows: list[dict[str, Any]] = []
    bytes_used = _json_size_bytes(dict(envelope_overhead or {}))
    row_dicts = [dict(row) for row in rows]

    if not enforce or max_bytes <= 0:
        for row in row_dicts:
            bytes_used += _json_size_bytes(row)
            bounded_rows.append(row)
        return ByteContainmentResult(
            rows=bounded_rows,
            partial=False,
            partial_reason=None,
            items_returned=len(bounded_rows),
            bytes_returned=bytes_used,
        )

    truncated = False
    for row in row_dicts:
        row_size = _json_size_bytes(row)
        if bytes_used + row_size > max_bytes:
            truncated = True
            break
        bounded_rows.append(row)
        bytes_used += row_size

    return ByteContainmentResult(
        rows=bounded_rows,
        partial=truncated,
        partial_reason=PayloadTruncationReason.MAX_BYTES.value if truncated else None,
        items_returned=len(bounded_rows),
        bytes_returned=bytes_used,
    )

"""Unit tests for offset cursor legacy migration behavior."""

from __future__ import annotations

import base64
import json

import pytest

from dal.offset_pagination import (
    OffsetPaginationTokenError,
    decode_offset_pagination_token,
    encode_offset_pagination_token,
)
from dal.pagination_cursor import PAGINATION_CURSOR_MIGRATION_UNSAFE

pytestmark = pytest.mark.pagination


def _encode_wrapper(wrapper: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def test_offset_legacy_v0_cursor_migrates_and_decodes() -> None:
    """Legacy v0 offset cursor should migrate to current payload contract."""
    legacy_token = _encode_wrapper(
        {
            "p": {
                "offset": 7,
                "limit": 3,
                "fingerprint": "fp-offset-v0",
                "issued_at": 1_700_000_000,
                "max_age_s": 120,
            }
        }
    )

    decoded = decode_offset_pagination_token(
        token=legacy_token,
        expected_fingerprint="fp-offset-v0",
        max_length=2048,
        now_epoch_seconds=1_700_000_090,
    )

    assert decoded.offset == 7
    assert decoded.limit == 3
    assert decoded.fingerprint == "fp-offset-v0"
    assert decoded.issued_at_ms == 1_700_000_000_000
    assert decoded.ttl_ms == 120_000


def test_offset_legacy_v0_cursor_missing_critical_fields_fails_closed() -> None:
    """Legacy offset cursor without required ttl data should reject as unsafe migration."""
    legacy_token = _encode_wrapper(
        {
            "p": {
                "offset": 7,
                "limit": 3,
                "fingerprint": "fp-offset-v0",
                "issued_at": 1_700_000_000,
            }
        }
    )

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=legacy_token,
            expected_fingerprint="fp-offset-v0",
            max_length=2048,
            now_epoch_seconds=1_700_000_090,
        )

    assert exc_info.value.reason_code == PAGINATION_CURSOR_MIGRATION_UNSAFE


def test_offset_migrated_legacy_cursor_matches_native_v1_paging_semantics() -> None:
    """Legacy migration should preserve offset/limit paging behavior parity."""
    legacy_token = _encode_wrapper(
        {
            "p": {
                "o": 40,
                "l": 10,
                "f": "fp-offset-parity",
                "issued_at_ms": 1_700_000_100_000,
                "ttl_ms": 300_000,
            }
        }
    )
    native_token = encode_offset_pagination_token(
        offset=40,
        limit=10,
        fingerprint="fp-offset-parity",
        issued_at_ms=1_700_000_100_000,
        ttl_ms=300_000,
    )

    decoded_legacy = decode_offset_pagination_token(
        token=legacy_token,
        expected_fingerprint="fp-offset-parity",
        max_length=2048,
        now_epoch_seconds=1_700_000_200,
    )
    decoded_native = decode_offset_pagination_token(
        token=native_token,
        expected_fingerprint="fp-offset-parity",
        max_length=2048,
        now_epoch_seconds=1_700_000_200,
    )

    assert decoded_legacy.offset == decoded_native.offset
    assert decoded_legacy.limit == decoded_native.limit
    assert decoded_legacy.fingerprint == decoded_native.fingerprint

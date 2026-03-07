"""Regression tests for post-migration security validation of cursors."""

from __future__ import annotations

import base64
import json

import pytest

from dal.keyset_pagination import KEYSET_CURSOR_ORDERBY_MISMATCH, decode_keyset_cursor
from dal.offset_pagination import OffsetPaginationTokenError, decode_offset_pagination_token
from dal.pagination_cursor import PAGINATION_CURSOR_EXPIRED, PAGINATION_CURSOR_SCOPE_MISMATCH

pytestmark = pytest.mark.pagination


def _encode_wrapper(wrapper: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def _encode_payload(payload: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def test_migrated_offset_cursor_rejected_on_scope_mismatch() -> None:
    """Migrated legacy offset cursor must still enforce scope binding checks."""
    legacy_token = _encode_wrapper(
        {
            "p": {
                "offset": 5,
                "limit": 2,
                "fingerprint": "fp-scope",
                "issued_at": 1_700_000_000,
                "max_age_s": 120,
                "scope_fp": "abcdeffedcba0123",
            }
        }
    )

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=legacy_token,
            expected_fingerprint="fp-scope",
            max_length=2048,
            expected_scope_fp="0123456789abcdef",
            now_epoch_seconds=1_700_000_010,
        )

    assert exc_info.value.reason_code == PAGINATION_CURSOR_SCOPE_MISMATCH


def test_migrated_offset_cursor_rejected_when_expired() -> None:
    """Migrated legacy offset cursor must still enforce ttl expiration checks."""
    legacy_token = _encode_wrapper(
        {
            "p": {
                "offset": 5,
                "limit": 2,
                "fingerprint": "fp-expired",
                "issued_at": 1_000,
                "max_age_s": 30,
            }
        }
    )

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=legacy_token,
            expected_fingerprint="fp-expired",
            max_length=2048,
            now_epoch_seconds=1_100,
        )

    assert exc_info.value.reason_code == PAGINATION_CURSOR_EXPIRED


def test_migrated_keyset_cursor_rejected_on_order_signature_mismatch() -> None:
    """Migrated legacy keyset cursor must still enforce canonical ORDER BY parity."""
    legacy_cursor = _encode_payload(
        {
            "v": [10, "row"],
            "k": ["created_at|desc|nulls_first", "id|asc|nulls_last"],
            "f": "fp-order",
            "issued_at": 1_700_000_000,
            "max_age_s": 300,
        }
    )

    with pytest.raises(ValueError, match=KEYSET_CURSOR_ORDERBY_MISMATCH):
        decode_keyset_cursor(
            legacy_cursor,
            expected_fingerprint="fp-order",
            expected_keys=["id|asc|nulls_last", "created_at|desc|nulls_first"],
            now_epoch_seconds=1_700_000_010,
        )

"""Unit tests for keyset cursor legacy migration behavior."""

from __future__ import annotations

import base64
import json

import pytest

from dal.keyset_pagination import (
    KEYSET_CURSOR_ORDERBY_MISMATCH,
    KEYSET_CURSOR_ORDERBY_SIGNATURE_MISSING,
    decode_keyset_cursor,
)

pytestmark = pytest.mark.pagination


def _encode_payload(payload: dict[str, object]) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")


def test_keyset_legacy_v0_cursor_migrates_and_decodes() -> None:
    """Legacy keyset payload should migrate to current envelope contract."""
    legacy_cursor = _encode_payload(
        {
            "v": [100, "abc"],
            "k": ["created_at|desc|nulls_first", "id|asc|nulls_last"],
            "f": "fp-keyset-v0",
            "issued_at": 1_700_000_000,
            "max_age_s": 300,
            "kid": "legacy",
        }
    )

    decoded = decode_keyset_cursor(
        legacy_cursor,
        expected_fingerprint="fp-keyset-v0",
        expected_keys=["created_at|desc|nulls_first", "id|asc|nulls_last"],
        now_epoch_seconds=1_700_000_100,
    )

    assert decoded == [100, "abc"]


def test_keyset_legacy_v0_cursor_missing_order_signature_fails_closed() -> None:
    """Legacy keyset payload without ORDER BY signature should reject fail closed."""
    legacy_cursor = _encode_payload(
        {
            "v": [100],
            "f": "fp-keyset-v0",
            "issued_at": 1_700_000_000,
            "max_age_s": 300,
            "kid": "legacy",
        }
    )

    with pytest.raises(ValueError, match=KEYSET_CURSOR_ORDERBY_SIGNATURE_MISSING):
        decode_keyset_cursor(
            legacy_cursor,
            expected_fingerprint="fp-keyset-v0",
            expected_keys=["id|asc|nulls_last"],
            now_epoch_seconds=1_700_000_100,
        )


def test_keyset_migrated_legacy_cursor_stays_bound_to_order_signature() -> None:
    """Migrated legacy cursors must still enforce canonical ORDER BY parity."""
    legacy_cursor = _encode_payload(
        {
            "v": [100, "abc"],
            "k": ["created_at|desc|nulls_first", "id|asc|nulls_last"],
            "f": "fp-keyset-v0",
            "issued_at": 1_700_000_000,
            "max_age_s": 300,
            "kid": "legacy",
        }
    )

    with pytest.raises(ValueError, match=KEYSET_CURSOR_ORDERBY_MISMATCH):
        decode_keyset_cursor(
            legacy_cursor,
            expected_fingerprint="fp-keyset-v0",
            expected_keys=["id|asc|nulls_last", "created_at|desc|nulls_first"],
            now_epoch_seconds=1_700_000_100,
        )

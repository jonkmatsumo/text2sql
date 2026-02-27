"""Unit tests for pagination cursor issued-at and legacy compatibility behavior."""

from __future__ import annotations

import base64
import json

import pytest

from dal.keyset_pagination import decode_keyset_cursor, encode_keyset_cursor
from dal.offset_pagination import (
    OffsetPaginationTokenError,
    decode_offset_pagination_token,
    encode_offset_pagination_token,
)


def _decode_base64_json(token: str) -> dict:
    padded = token + "=" * (-len(token) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))


def test_offset_cursor_encoding_includes_issued_at_with_injected_clock():
    """Offset cursor encoding should populate deterministic issued_at metadata."""
    token = encode_offset_pagination_token(
        offset=10,
        limit=5,
        fingerprint="fp1",
        now_epoch_seconds=1_700_000_001,
        max_age_s=123,
    )

    wrapper = _decode_base64_json(token)
    payload = wrapper["p"]
    assert payload["issued_at"] == 1_700_000_001
    assert payload["max_age_s"] == 123


def test_keyset_cursor_encoding_includes_issued_at_with_injected_clock():
    """Keyset cursor encoding should populate deterministic issued_at metadata."""
    cursor = encode_keyset_cursor(
        [123],
        ["id|asc|nulls_last"],
        "fp1",
        now_epoch_seconds=1_700_000_002,
        max_age_s=321,
    )
    payload = _decode_base64_json(cursor)
    assert payload["issued_at"] == 1_700_000_002
    assert payload["max_age_s"] == 321


def test_offset_decode_accepts_new_cursor_with_issued_at():
    """Offset decode should accept new issued_at-bearing payloads by default."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=1_700_000_100,
    )
    decoded = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp1",
        max_length=2048,
    )
    assert decoded.offset == 1
    assert decoded.limit == 2
    assert decoded.issued_at == 1_700_000_100
    assert decoded.legacy_issued_at_accepted is False


def test_keyset_decode_accepts_new_cursor_with_issued_at():
    """Keyset decode should accept new issued_at-bearing payloads by default."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_700_000_101,
    )
    decoded = decode_keyset_cursor(
        cursor,
        expected_fingerprint="fp1",
        expected_keys=["id|asc|nulls_last"],
    )
    assert decoded == [1]


def test_offset_decode_legacy_cursor_requires_flag():
    """Legacy offset tokens without issued_at should fail closed unless explicitly allowed."""
    legacy_payload = {"p": {"v": 1, "o": 1, "l": 2, "f": "fp1"}}
    legacy_token = base64.urlsafe_b64encode(json.dumps(legacy_payload).encode("utf-8")).decode(
        "ascii"
    )
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=legacy_token,
            expected_fingerprint="fp1",
            max_length=2048,
            require_issued_at=True,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_ISSUED_AT_INVALID"

    decode_metadata: dict[str, bool] = {}
    decoded = decode_offset_pagination_token(
        token=legacy_token,
        expected_fingerprint="fp1",
        max_length=2048,
        require_issued_at=False,
        decode_metadata=decode_metadata,
    )
    assert decoded.offset == 1
    assert decoded.legacy_issued_at_accepted is True
    assert decode_metadata.get("legacy_issued_at_accepted") is True


def test_keyset_decode_legacy_cursor_requires_flag():
    """Legacy keyset cursors without issued_at should fail closed unless explicitly allowed."""
    legacy_payload = {"v": [1], "k": ["id|asc|nulls_last"], "f": "fp1"}
    legacy_cursor = base64.urlsafe_b64encode(json.dumps(legacy_payload).encode("utf-8")).decode(
        "ascii"
    )
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_ISSUED_AT_INVALID"):
        decode_keyset_cursor(
            legacy_cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
            require_issued_at=True,
        )

    decode_metadata: dict[str, bool] = {}
    decoded = decode_keyset_cursor(
        legacy_cursor,
        expected_fingerprint="fp1",
        expected_keys=["id|asc|nulls_last"],
        require_issued_at=False,
        decode_metadata=decode_metadata,
    )
    assert decoded == [1]
    assert decode_metadata.get("legacy_issued_at_accepted") is True

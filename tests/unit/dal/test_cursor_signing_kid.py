"""Unit tests for pagination cursor signing key-id (kid) metadata behavior."""

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
from dal.pagination_cursor import CURSOR_KID_MAX_LENGTH, PAGINATION_CURSOR_KID_MISSING

pytestmark = pytest.mark.pagination


def _decode_base64_json(token: str) -> dict:
    """Decode URL-safe base64 JSON payloads used by cursor contracts."""
    padded = token + "=" * (-len(token) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))


def test_offset_encode_includes_kid_metadata_by_default() -> None:
    """Offset cursor payloads should include the signing key id by default."""
    token = encode_offset_pagination_token(offset=0, limit=10, fingerprint="fp")
    wrapper = _decode_base64_json(token)
    assert wrapper["p"]["kid"] == "legacy"


def test_keyset_encode_includes_kid_metadata_by_default() -> None:
    """Keyset cursor payloads should include the signing key id by default."""
    cursor = encode_keyset_cursor([1], ["id|asc|nulls_last"], "fp")
    payload = _decode_base64_json(cursor)
    assert payload["kid"] == "legacy"


def test_offset_decode_rejects_missing_kid_fail_closed() -> None:
    """Offset decode should fail closed when kid metadata is absent."""
    token = encode_offset_pagination_token(offset=0, limit=10, fingerprint="fp")
    wrapper = _decode_base64_json(token)
    wrapper["p"].pop("kid", None)
    tampered_token = base64.urlsafe_b64encode(
        json.dumps(wrapper, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=tampered_token,
            expected_fingerprint="fp",
            max_length=2048,
        )

    assert exc_info.value.reason_code == PAGINATION_CURSOR_KID_MISSING


def test_keyset_decode_rejects_missing_kid_fail_closed() -> None:
    """Keyset decode should fail closed when kid metadata is absent."""
    cursor = encode_keyset_cursor([1], ["id|asc|nulls_last"], "fp")
    payload = _decode_base64_json(cursor)
    payload.pop("kid", None)
    tampered_cursor = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii")

    with pytest.raises(ValueError, match=PAGINATION_CURSOR_KID_MISSING):
        decode_keyset_cursor(
            tampered_cursor,
            expected_fingerprint="fp",
            expected_keys=["id|asc|nulls_last"],
        )


def test_kid_sanitization_and_length_bounds() -> None:
    """Kid metadata should be normalized and bounded for both cursor modes."""
    token = encode_offset_pagination_token(
        offset=0,
        limit=10,
        fingerprint="fp",
        kid="  ACTIVE.KEY_01  ",
    )
    wrapper = _decode_base64_json(token)
    assert wrapper["p"]["kid"] == "active.key_01"

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        encode_offset_pagination_token(
            offset=0,
            limit=10,
            fingerprint="fp",
            kid="a" * (CURSOR_KID_MAX_LENGTH + 1),
        )
    assert exc_info.value.reason_code == PAGINATION_CURSOR_KID_MISSING

    with pytest.raises(ValueError, match=PAGINATION_CURSOR_KID_MISSING):
        encode_keyset_cursor(
            [1],
            ["id|asc|nulls_last"],
            "fp",
            kid="invalid kid",
        )

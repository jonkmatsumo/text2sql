"""Parity tests for cursor signature failure classification across keyset and offset flows."""

from __future__ import annotations

import pytest

from dal.keyset_pagination import (
    PAGINATION_CURSOR_SIGNATURE_INVALID,
    decode_keyset_cursor,
    encode_keyset_cursor,
)
from dal.offset_pagination import (
    OffsetPaginationTokenError,
    decode_offset_pagination_token,
    encode_offset_pagination_token,
)
from dal.pagination_cursor import PAGINATION_CURSOR_SIGNATURE_INVALID as SHARED_REASON_CODE

pytestmark = pytest.mark.pagination


def test_keyset_signature_invalid_uses_stable_reason_code():
    """Keyset decode with wrong secret emits PAGINATION_CURSOR_SIGNATURE_INVALID."""
    cursor = encode_keyset_cursor(
        [1], ["id"], "fp", secret="correct-secret", now_epoch_seconds=1000
    )
    decode_metadata: dict = {}
    with pytest.raises(ValueError, match=PAGINATION_CURSOR_SIGNATURE_INVALID):
        decode_keyset_cursor(
            cursor,
            "fp",
            secret="wrong-secret",
            decode_metadata=decode_metadata,
            now_epoch_seconds=1000,
        )
    assert decode_metadata.get("validation_outcome") == "SIGNATURE_INVALID"


def test_keyset_missing_signature_uses_stable_reason_code():
    """Keyset decode of unsigned cursor with secret set emits SIGNATURE_INVALID."""
    cursor = encode_keyset_cursor([1], ["id"], "fp", now_epoch_seconds=1000)
    decode_metadata: dict = {}
    with pytest.raises(ValueError, match=PAGINATION_CURSOR_SIGNATURE_INVALID):
        decode_keyset_cursor(
            cursor,
            "fp",
            secret="some-secret",
            decode_metadata=decode_metadata,
            now_epoch_seconds=1000,
        )
    assert decode_metadata.get("validation_outcome") == "SIGNATURE_INVALID"


def test_offset_signature_invalid_uses_stable_reason_code():
    """Offset decode with wrong secret emits PAGINATION_CURSOR_SIGNATURE_INVALID."""
    token = encode_offset_pagination_token(
        offset=0, limit=10, fingerprint="fp", secret="correct-secret", now_epoch_seconds=1000
    )
    decode_metadata: dict = {}
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp",
            max_length=4096,
            secret="wrong-secret",
            decode_metadata=decode_metadata,
            now_epoch_seconds=1000,
        )
    assert exc_info.value.reason_code == PAGINATION_CURSOR_SIGNATURE_INVALID
    assert decode_metadata.get("validation_outcome") == "SIGNATURE_INVALID"


def test_offset_missing_signature_uses_stable_reason_code():
    """Offset decode of unsigned token with secret set emits SIGNATURE_INVALID."""
    token = encode_offset_pagination_token(
        offset=0, limit=10, fingerprint="fp", now_epoch_seconds=1000
    )
    decode_metadata: dict = {}
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp",
            max_length=4096,
            secret="some-secret",
            decode_metadata=decode_metadata,
            now_epoch_seconds=1000,
        )
    assert exc_info.value.reason_code == PAGINATION_CURSOR_SIGNATURE_INVALID
    assert decode_metadata.get("validation_outcome") == "SIGNATURE_INVALID"


def test_keyset_and_offset_signature_reason_codes_identical():
    """Keyset and offset use the same PAGINATION_CURSOR_SIGNATURE_INVALID constant."""
    assert PAGINATION_CURSOR_SIGNATURE_INVALID == SHARED_REASON_CODE
    assert PAGINATION_CURSOR_SIGNATURE_INVALID == "PAGINATION_CURSOR_SIGNATURE_INVALID"


def test_keyset_signature_error_does_not_leak_cursor(monkeypatch):
    """Signature error message must not include raw cursor payload."""
    cursor = encode_keyset_cursor(
        [1, "sensitive_data"],
        ["id", "name"],
        "fp",
        secret="correct",
        now_epoch_seconds=1000,
    )
    with pytest.raises(ValueError) as exc_info:
        decode_keyset_cursor(cursor, "fp", secret="wrong", now_epoch_seconds=1000)
    msg = str(exc_info.value)
    assert "sensitive_data" not in msg
    assert cursor not in msg


def test_offset_signature_error_does_not_leak_cursor(monkeypatch):
    """Signature error message must not include raw cursor payload."""
    token = encode_offset_pagination_token(
        offset=42,
        limit=10,
        fingerprint="fp",
        secret="correct",
        now_epoch_seconds=1000,
    )
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp",
            max_length=4096,
            secret="wrong",
            now_epoch_seconds=1000,
        )
    msg = str(exc_info.value)
    assert token not in msg

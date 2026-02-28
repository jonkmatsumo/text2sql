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

pytestmark = pytest.mark.pagination


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
        now_epoch_seconds=1_700_000_150,
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
        now_epoch_seconds=1_700_000_150,
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


def test_offset_decode_rejects_expired_cursor():
    """Offset cursor should fail closed when the issued-at age exceeds max_age."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=1_000,
        max_age_s=100,
    )
    decode_metadata: dict[str, object] = {}
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp1",
            max_length=2048,
            now_epoch_seconds=1_101,
            decode_metadata=decode_metadata,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_EXPIRED"
    assert decode_metadata.get("expired") is True
    assert decode_metadata.get("validation_outcome") == "EXPIRED"


def test_keyset_decode_rejects_expired_cursor():
    """Keyset cursor should fail closed when the issued-at age exceeds max_age."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_000,
        max_age_s=100,
    )
    decode_metadata: dict[str, object] = {}
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_EXPIRED"):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
            now_epoch_seconds=1_101,
            decode_metadata=decode_metadata,
        )
    assert decode_metadata.get("expired") is True
    assert decode_metadata.get("validation_outcome") == "EXPIRED"


def test_offset_decode_rejects_future_issued_at_clock_skew():
    """Offset cursor should fail closed when issued_at is too far in the future."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=2_000,
        max_age_s=300,
    )
    decode_metadata: dict[str, object] = {}
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp1",
            max_length=2048,
            now_epoch_seconds=1_000,
            clock_skew_seconds=60,
            decode_metadata=decode_metadata,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_CLOCK_SKEW"
    assert decode_metadata.get("skew_detected") is True
    assert decode_metadata.get("validation_outcome") == "SKEW"


def test_keyset_decode_rejects_future_issued_at_clock_skew():
    """Keyset cursor should fail closed when issued_at is too far in the future."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=2_000,
        max_age_s=300,
    )
    decode_metadata: dict[str, object] = {}
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_CLOCK_SKEW"):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
            now_epoch_seconds=1_000,
            clock_skew_seconds=60,
            decode_metadata=decode_metadata,
        )
    assert decode_metadata.get("skew_detected") is True
    assert decode_metadata.get("validation_outcome") == "SKEW"


def test_offset_decode_rejects_non_integer_issued_at():
    """Offset cursor should reject non-integer issued_at payload values."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    wrapper = _decode_base64_json(token)
    wrapper["p"]["issued_at"] = "abc"
    invalid_token = base64.urlsafe_b64encode(json.dumps(wrapper).encode("utf-8")).decode("ascii")
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=invalid_token,
            expected_fingerprint="fp1",
            max_length=2048,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_ISSUED_AT_INVALID"


def test_keyset_decode_rejects_non_integer_issued_at():
    """Keyset cursor should reject non-integer issued_at payload values."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    payload = _decode_base64_json(cursor)
    payload["issued_at"] = "abc"
    invalid_cursor = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_ISSUED_AT_INVALID"):
        decode_keyset_cursor(
            invalid_cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
        )


def test_offset_decode_accepts_cursor_within_ttl_window():
    """Offset cursor should decode successfully when age is within configured TTL."""
    token = encode_offset_pagination_token(
        offset=5,
        limit=10,
        fingerprint="fp1",
        issued_at=1_000,
    )
    decoded = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp1",
        max_length=2048,
        max_age_seconds=100,
        now_epoch_seconds=1_099,
    )
    assert decoded.offset == 5
    assert decoded.limit == 10


def test_keyset_decode_accepts_cursor_within_ttl_window():
    """Keyset cursor should decode successfully when age is within configured TTL."""
    cursor = encode_keyset_cursor(
        [5],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_000,
    )
    decoded = decode_keyset_cursor(
        cursor,
        expected_fingerprint="fp1",
        expected_keys=["id|asc|nulls_last"],
        max_age_seconds=100,
        now_epoch_seconds=1_099,
    )
    assert decoded == [5]


def test_offset_decode_rejects_query_fingerprint_mismatch_in_strict_mode():
    """Offset cursor should reject strict query fingerprint mismatches."""
    token = encode_offset_pagination_token(
        offset=5,
        limit=10,
        fingerprint="fp1",
        issued_at=1_000,
        query_fp="query-a",
    )
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp1",
            expected_query_fp="query-b",
            max_length=2048,
            max_age_seconds=100,
            now_epoch_seconds=1_050,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_QUERY_MISMATCH"


def test_keyset_decode_rejects_query_fingerprint_mismatch_in_strict_mode():
    """Keyset cursor should reject strict query fingerprint mismatches."""
    cursor = encode_keyset_cursor(
        [5],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_000,
        query_fp="query-a",
    )
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_QUERY_MISMATCH"):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
            expected_query_fp="query-b",
            max_age_seconds=100,
            now_epoch_seconds=1_050,
        )

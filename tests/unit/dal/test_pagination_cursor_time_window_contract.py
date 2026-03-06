"""Unit tests for pagination cursor signed TTL metadata behavior."""

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
    """Offset cursor encoding should populate deterministic issued_at_ms/ttl_ms metadata."""
    token = encode_offset_pagination_token(
        offset=10,
        limit=5,
        fingerprint="fp1",
        now_epoch_seconds=1_700_000_001,
        max_age_s=123,
    )

    wrapper = _decode_base64_json(token)
    payload = wrapper["p"]
    assert payload["issued_at_ms"] == 1_700_000_001_000
    assert payload["ttl_ms"] == 123_000
    assert payload["issued_at"] == 1_700_000_001
    assert payload["max_age_s"] == 123


def test_keyset_cursor_encoding_includes_issued_at_with_injected_clock():
    """Keyset cursor encoding should populate deterministic issued_at_ms/ttl_ms metadata."""
    cursor = encode_keyset_cursor(
        [123],
        ["id|asc|nulls_last"],
        "fp1",
        now_epoch_seconds=1_700_000_002,
        max_age_s=321,
    )
    payload = _decode_base64_json(cursor)
    assert payload["issued_at_ms"] == 1_700_000_002_000
    assert payload["ttl_ms"] == 321_000
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
    assert decoded.issued_at_ms == 1_700_000_100_000
    assert decoded.ttl_ms == 3_600_000
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


def test_offset_decode_rejects_missing_ttl_metadata_fail_closed():
    """Offset tokens without required ttl metadata should fail closed."""
    legacy_payload = {"p": {"v": 1, "o": 1, "l": 2, "f": "fp1"}}
    legacy_token = base64.urlsafe_b64encode(json.dumps(legacy_payload).encode("utf-8")).decode(
        "ascii"
    )
    decode_metadata: dict[str, bool] = {}
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=legacy_token,
            expected_fingerprint="fp1",
            max_length=2048,
            decode_metadata=decode_metadata,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_TTL_MISSING"
    assert decode_metadata.get("validation_outcome") == "INVALID"


def test_keyset_decode_rejects_missing_ttl_metadata_fail_closed():
    """Keyset cursors without required ttl metadata should fail closed."""
    legacy_payload = {"v": [1], "k": ["id|asc|nulls_last"], "f": "fp1"}
    legacy_cursor = base64.urlsafe_b64encode(json.dumps(legacy_payload).encode("utf-8")).decode(
        "ascii"
    )
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_TTL_MISSING"):
        decode_keyset_cursor(
            legacy_cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
        )


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


def test_offset_decode_rejects_invalid_ttl_metadata_types():
    """Offset cursor should reject non-integer issued_at_ms payload values."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    wrapper = _decode_base64_json(token)
    wrapper["p"]["issued_at_ms"] = "abc"
    invalid_token = base64.urlsafe_b64encode(json.dumps(wrapper).encode("utf-8")).decode("ascii")
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=invalid_token,
            expected_fingerprint="fp1",
            max_length=2048,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_TTL_INVALID"


def test_keyset_decode_rejects_invalid_ttl_metadata_types():
    """Keyset cursor should reject non-integer issued_at_ms payload values."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    payload = _decode_base64_json(cursor)
    payload["issued_at_ms"] = "abc"
    invalid_cursor = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_TTL_INVALID"):
        decode_keyset_cursor(
            invalid_cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
        )


def test_offset_decode_rejects_negative_ttl_metadata_values():
    """Offset cursor should reject negative ttl_ms metadata values."""
    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    wrapper = _decode_base64_json(token)
    wrapper["p"]["ttl_ms"] = -1
    invalid_token = base64.urlsafe_b64encode(json.dumps(wrapper).encode("utf-8")).decode("ascii")
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=invalid_token,
            expected_fingerprint="fp1",
            max_length=2048,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_TTL_INVALID"


def test_keyset_decode_rejects_overflow_ttl_metadata_values():
    """Keyset cursor should reject overflow ttl_ms metadata values."""
    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_700_000_100,
        max_age_s=300,
    )
    payload = _decode_base64_json(cursor)
    payload["ttl_ms"] = 2**80
    invalid_cursor = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_TTL_INVALID"):
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


def test_offset_decode_replay_guard_disabled_allows_cursor_reuse():
    """Replay guard disabled should preserve cursor reuse behavior."""
    token = encode_offset_pagination_token(
        offset=5,
        limit=10,
        fingerprint="fp1",
        issued_at=1_000,
        max_age_s=300,
    )
    first = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp1",
        max_length=2048,
        replay_guard_enabled=False,
        now_epoch_seconds=1_050,
    )
    second = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp1",
        max_length=2048,
        replay_guard_enabled=False,
        now_epoch_seconds=1_051,
    )
    assert first.offset == 5
    assert second.offset == 5


def test_offset_decode_replay_guard_enabled_rejects_second_use():
    """Replay guard enabled should reject second decode of the same offset cursor."""
    token = encode_offset_pagination_token(
        offset=5,
        limit=10,
        fingerprint="fp1",
        issued_at=1_000,
        max_age_s=300,
    )
    decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp1",
        max_length=2048,
        replay_guard_enabled=True,
        now_epoch_seconds=1_050,
    )
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp1",
            max_length=2048,
            replay_guard_enabled=True,
            now_epoch_seconds=1_051,
        )
    assert exc_info.value.reason_code == "PAGINATION_CURSOR_REPLAY_DETECTED"


def test_keyset_decode_replay_guard_enabled_rejects_second_use():
    """Replay guard enabled should reject second decode of the same keyset cursor."""
    cursor = encode_keyset_cursor(
        [5],
        ["id|asc|nulls_last"],
        "fp1",
        issued_at=1_000,
        max_age_s=300,
    )
    decode_keyset_cursor(
        cursor,
        expected_fingerprint="fp1",
        expected_keys=["id|asc|nulls_last"],
        replay_guard_enabled=True,
        now_epoch_seconds=1_050,
    )
    with pytest.raises(ValueError, match="PAGINATION_CURSOR_REPLAY_DETECTED"):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="fp1",
            expected_keys=["id|asc|nulls_last"],
            replay_guard_enabled=True,
            now_epoch_seconds=1_051,
        )


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

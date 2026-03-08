"""Unit tests for bounded cursor signing keyring rotation behavior."""

from __future__ import annotations

import json

import pytest

from dal.keyset_pagination import decode_keyset_cursor, encode_keyset_cursor
from dal.offset_pagination import (
    OffsetPaginationTokenError,
    decode_offset_pagination_token,
    encode_offset_pagination_token,
)
from dal.pagination_cursor import (
    PAGINATION_CURSOR_KEYRING_INVALID,
    PAGINATION_CURSOR_KID_UNKNOWN,
    CursorSigningKeyring,
)

pytestmark = pytest.mark.pagination

_ACTIVE_SECRET = "test-pagination-secret-for-unit-tests-2026"
_PREVIOUS_SECRET = "test-pagination-secret-for-unit-tests-2025"
_UNKNOWN_SECRET = "test-pagination-secret-for-unit-tests-2024"


def _rotation_keyring() -> CursorSigningKeyring:
    """Build a valid active+previous keyring used by rotation tests."""
    return CursorSigningKeyring(
        active_kid="active",
        keys={
            "active": _ACTIVE_SECRET,
            "previous": _PREVIOUS_SECRET,
        },
        configured=True,
        valid=True,
        reason_code=None,
        source_env_var="PAGINATION_CURSOR_SIGNING_KEYS_JSON",
    )


def test_active_key_encode_decode_succeeds_across_offset_and_keyset() -> None:
    """Active key should sign and verify both offset and keyset cursors."""
    keyring = _rotation_keyring()

    token = encode_offset_pagination_token(
        offset=0,
        limit=10,
        fingerprint="fp-offset-active",
        signing_keyring=keyring,
    )
    decoded_token = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp-offset-active",
        max_length=2048,
        signing_keyring=keyring,
    )
    assert decoded_token.offset == 0

    cursor = encode_keyset_cursor(
        [1],
        ["id|asc|nulls_last"],
        "fp-keyset-active",
        signing_keyring=keyring,
    )
    decoded_cursor = decode_keyset_cursor(
        cursor,
        expected_fingerprint="fp-keyset-active",
        expected_keys=["id|asc|nulls_last"],
        signing_keyring=keyring,
    )
    assert decoded_cursor == [1]


def test_previous_key_decode_succeeds_during_rotation_window() -> None:
    """Cursors signed with a previous allowed key should continue to decode."""
    keyring = _rotation_keyring()

    token = encode_offset_pagination_token(
        offset=3,
        limit=5,
        fingerprint="fp-offset-previous",
        secret=_PREVIOUS_SECRET,
        kid="previous",
    )
    decoded_token = decode_offset_pagination_token(
        token=token,
        expected_fingerprint="fp-offset-previous",
        max_length=2048,
        signing_keyring=keyring,
    )
    assert decoded_token.offset == 3

    cursor = encode_keyset_cursor(
        [7],
        ["id|asc|nulls_last"],
        "fp-keyset-previous",
        secret=_PREVIOUS_SECRET,
        kid="previous",
    )
    decoded_cursor = decode_keyset_cursor(
        cursor,
        expected_fingerprint="fp-keyset-previous",
        expected_keys=["id|asc|nulls_last"],
        signing_keyring=keyring,
    )
    assert decoded_cursor == [7]


def test_unknown_kid_rejected_fail_closed() -> None:
    """Unknown kid values should be rejected without trying unrelated secrets."""
    keyring = _rotation_keyring()

    token = encode_offset_pagination_token(
        offset=1,
        limit=2,
        fingerprint="fp-offset-unknown",
        secret=_UNKNOWN_SECRET,
        kid="unknown",
    )
    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        decode_offset_pagination_token(
            token=token,
            expected_fingerprint="fp-offset-unknown",
            max_length=2048,
            signing_keyring=keyring,
        )
    assert exc_info.value.reason_code == PAGINATION_CURSOR_KID_UNKNOWN

    cursor = encode_keyset_cursor(
        [9],
        ["id|asc|nulls_last"],
        "fp-keyset-unknown",
        secret=_UNKNOWN_SECRET,
        kid="unknown",
    )
    with pytest.raises(ValueError, match=PAGINATION_CURSOR_KID_UNKNOWN):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="fp-keyset-unknown",
            expected_keys=["id|asc|nulls_last"],
            signing_keyring=keyring,
        )


def test_invalid_keyring_rejected_fail_closed_from_env(monkeypatch) -> None:
    """Misconfigured keyring env must fail closed with keyring-invalid classification."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_ACTIVE_KID", "active")
    monkeypatch.setenv(
        "PAGINATION_CURSOR_SIGNING_KEYS_JSON",
        json.dumps({"previous": _PREVIOUS_SECRET}),
    )

    resolved = CursorSigningKeyring.from_env()
    assert resolved.valid is False
    assert resolved.reason_code == PAGINATION_CURSOR_KEYRING_INVALID

    with pytest.raises(OffsetPaginationTokenError) as exc_info:
        encode_offset_pagination_token(
            offset=0,
            limit=10,
            fingerprint="fp-misconfigured",
            signing_keyring=resolved,
        )
    assert exc_info.value.reason_code == PAGINATION_CURSOR_KEYRING_INVALID


def test_keyring_size_is_bounded_fail_closed(monkeypatch) -> None:
    """Oversized keyrings should be rejected as invalid configuration."""
    monkeypatch.setenv("PAGINATION_CURSOR_SIGNING_ACTIVE_KID", "k0")
    monkeypatch.setenv(
        "PAGINATION_CURSOR_SIGNING_KEYS_JSON",
        json.dumps({f"k{i}": _ACTIVE_SECRET for i in range(9)}),
    )
    resolved = CursorSigningKeyring.from_env()
    assert resolved.valid is False
    assert resolved.reason_code == PAGINATION_CURSOR_KEYRING_INVALID


def test_removed_key_is_rejected_after_rotation_window_for_offset_and_keyset() -> None:
    """Removing a previous key should retire existing cursors for both pagination modes."""
    active_and_previous = _rotation_keyring()
    active_only = CursorSigningKeyring(
        active_kid="active",
        keys={"active": _ACTIVE_SECRET},
        configured=True,
        valid=True,
        reason_code=None,
        source_env_var="PAGINATION_CURSOR_SIGNING_KEYS_JSON",
    )

    previous_offset_token = encode_offset_pagination_token(
        offset=11,
        limit=4,
        fingerprint="fp-offset-retired",
        secret=_PREVIOUS_SECRET,
        kid="previous",
    )
    previous_keyset_cursor = encode_keyset_cursor(
        [11],
        ["id|asc|nulls_last"],
        "fp-keyset-retired",
        secret=_PREVIOUS_SECRET,
        kid="previous",
    )

    # During rotation window, both cursors still decode via the secondary key.
    assert (
        decode_offset_pagination_token(
            token=previous_offset_token,
            expected_fingerprint="fp-offset-retired",
            max_length=2048,
            signing_keyring=active_and_previous,
        ).offset
        == 11
    )
    assert decode_keyset_cursor(
        previous_keyset_cursor,
        expected_fingerprint="fp-keyset-retired",
        expected_keys=["id|asc|nulls_last"],
        signing_keyring=active_and_previous,
    ) == [11]

    # After retirement (previous key removed), both fail closed with KID_UNKNOWN.
    with pytest.raises(OffsetPaginationTokenError) as offset_exc:
        decode_offset_pagination_token(
            token=previous_offset_token,
            expected_fingerprint="fp-offset-retired",
            max_length=2048,
            signing_keyring=active_only,
        )
    assert offset_exc.value.reason_code == PAGINATION_CURSOR_KID_UNKNOWN

    with pytest.raises(ValueError, match=PAGINATION_CURSOR_KID_UNKNOWN):
        decode_keyset_cursor(
            previous_keyset_cursor,
            expected_fingerprint="fp-keyset-retired",
            expected_keys=["id|asc|nulls_last"],
            signing_keyring=active_only,
        )

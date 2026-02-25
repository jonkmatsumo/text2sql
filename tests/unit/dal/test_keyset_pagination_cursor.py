import pytest

from dal.keyset_pagination import KEYSET_ORDER_MISMATCH, decode_keyset_cursor, encode_keyset_cursor


def test_keyset_cursor_roundtrip():
    """Test successful encoding and decoding of a keyset cursor."""
    values = [123, "abc"]
    keys = ["id", "name"]
    fingerprint = "test-fingerprint"
    secret = "test-secret"

    cursor = encode_keyset_cursor(values, keys, fingerprint, secret=secret)
    decoded_values = decode_keyset_cursor(cursor, expected_fingerprint=fingerprint, secret=secret)

    assert decoded_values == values


def test_keyset_cursor_fingerprint_mismatch():
    """Test that cursor decoding fails if the fingerprint does not match."""
    values = [123]
    keys = ["id"]
    fingerprint = "fingerprint-1"

    cursor = encode_keyset_cursor(values, keys, fingerprint)

    with pytest.raises(ValueError, match="fingerprint mismatch"):
        decode_keyset_cursor(cursor, expected_fingerprint="fingerprint-2")


def test_keyset_cursor_invalid_format():
    """Test that decoding an invalid base64 string fails gracefully."""
    with pytest.raises(ValueError, match="Failed to decode cursor"):
        decode_keyset_cursor("invalid-base64-!!!", expected_fingerprint="any")


def test_keyset_cursor_secret_validation():
    """Test that cursor signatures are validated using a secret."""
    values = [123]
    keys = ["id"]
    fingerprint = "f1"
    secret = "s1"

    cursor = encode_keyset_cursor(values, keys, fingerprint, secret=secret)

    # Success with correct secret
    assert decode_keyset_cursor(cursor, fingerprint, secret=secret) == values

    # Failure with wrong secret
    with pytest.raises(ValueError, match="signature mismatch"):
        decode_keyset_cursor(cursor, fingerprint, secret="wrong-secret")

    # Failure if secret expected but missing in cursor
    cursor_no_secret = encode_keyset_cursor(values, keys, fingerprint)
    with pytest.raises(ValueError, match="signature mismatch"):
        decode_keyset_cursor(cursor_no_secret, fingerprint, secret=secret)


def test_keyset_cursor_order_signature_rejects_direction_change():
    """Changing ORDER BY direction between pages must be rejected."""
    cursor = encode_keyset_cursor([123], ["id|asc|nulls_last"], "f1")
    with pytest.raises(ValueError, match=KEYSET_ORDER_MISMATCH):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="f1",
            expected_keys=["id|desc|nulls_first"],
        )


def test_keyset_cursor_order_signature_rejects_added_or_removed_key():
    """Changing ORDER BY key count between pages must be rejected."""
    cursor = encode_keyset_cursor([123], ["created_at|desc|nulls_first"], "f1")
    with pytest.raises(ValueError, match=KEYSET_ORDER_MISMATCH):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="f1",
            expected_keys=["created_at|desc|nulls_first", "id|asc|nulls_last"],
        )


def test_keyset_cursor_order_signature_rejects_reordered_keys():
    """Changing ORDER BY key order between pages must be rejected."""
    cursor = encode_keyset_cursor(
        [123, 456],
        ["created_at|desc|nulls_first", "id|asc|nulls_last"],
        "f1",
    )
    with pytest.raises(ValueError, match=KEYSET_ORDER_MISMATCH):
        decode_keyset_cursor(
            cursor,
            expected_fingerprint="f1",
            expected_keys=["id|asc|nulls_last", "created_at|desc|nulls_first"],
        )


def test_keyset_cursor_order_signature_accepts_same_structure():
    """Matching ORDER BY structure should decode successfully."""
    cursor = encode_keyset_cursor(
        [123, 456],
        ["created_at|desc|nulls_first", "id|asc|nulls_last"],
        "f1",
    )
    decoded = decode_keyset_cursor(
        cursor,
        expected_fingerprint="f1",
        expected_keys=["created_at|desc|nulls_first", "id|asc|nulls_last"],
    )
    assert decoded == [123, 456]

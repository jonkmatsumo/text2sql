import pytest

from dal.keyset_pagination import decode_keyset_cursor, encode_keyset_cursor


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

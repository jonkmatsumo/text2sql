"""Deterministic hashing utilities for stable data generation."""

import hashlib
import struct


def stable_hash_bytes(data: bytes) -> str:
    """Compute a stable SHA-256 hash of bytes, returning hex string.

    Args:
        data: Raw bytes to hash.

    Returns:
        Lowercase hexadecimal string of the SHA-256 digest.
    """
    return hashlib.sha256(data).hexdigest()


def stable_hash_str(s: str) -> str:
    """Compute a stable SHA-256 hash of a string, returning hex string.

    Uses UTF-8 encoding for consistency across platforms.

    Args:
        s: String to hash.

    Returns:
        Lowercase hexadecimal string of the SHA-256 digest.
    """
    return stable_hash_bytes(s.encode("utf-8"))


def seed_from_str(s: str) -> int:
    """Derive a 32-bit unsigned integer seed from a string.

    Uses the first 4 bytes of the SHA-256 hash interpreted as
    a big-endian unsigned integer.

    Args:
        s: String to derive seed from.

    Returns:
        32-bit unsigned integer suitable for RNG seeding.
    """
    hash_bytes = hashlib.sha256(s.encode("utf-8")).digest()
    # Use first 4 bytes as big-endian unsigned int
    return struct.unpack(">I", hash_bytes[:4])[0]


def stable_row_hash(row_data: dict) -> str:
    """Compute a stable hash for a row of data.

    Sorts keys to ensure deterministic ordering, then hashes
    the string representation.

    Args:
        row_data: Dictionary of column name -> value.

    Returns:
        Lowercase hexadecimal string of the SHA-256 digest.
    """
    # Sort keys for deterministic ordering
    sorted_items = sorted(row_data.items(), key=lambda x: x[0])
    # Create stable string representation
    repr_str = "|".join(f"{k}={v!r}" for k, v in sorted_items)
    return stable_hash_str(repr_str)

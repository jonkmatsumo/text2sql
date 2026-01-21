"""Tests for hashing utilities."""

from text2sql_synth.util.hashing import (
    seed_from_str,
    stable_hash_bytes,
    stable_hash_str,
    stable_row_hash,
)


class TestStableHashBytes:
    """Tests for stable_hash_bytes function."""

    def test_deterministic(self) -> None:
        """Same input produces same output."""
        data = b"test data"
        hash1 = stable_hash_bytes(data)
        hash2 = stable_hash_bytes(data)
        assert hash1 == hash2

    def test_different_input_different_hash(self) -> None:
        """Different inputs produce different hashes."""
        hash1 = stable_hash_bytes(b"data1")
        hash2 = stable_hash_bytes(b"data2")
        assert hash1 != hash2

    def test_returns_hex_string(self) -> None:
        """Returns a valid hex string."""
        result = stable_hash_bytes(b"test")
        assert isinstance(result, str)
        assert len(result) == 64  # SHA-256 = 256 bits = 64 hex chars
        assert all(c in "0123456789abcdef" for c in result)


class TestStableHashStr:
    """Tests for stable_hash_str function."""

    def test_deterministic(self) -> None:
        """Same string produces same hash."""
        hash1 = stable_hash_str("hello world")
        hash2 = stable_hash_str("hello world")
        assert hash1 == hash2

    def test_unicode_handling(self) -> None:
        """Unicode strings are handled consistently."""
        hash1 = stable_hash_str("café")
        hash2 = stable_hash_str("café")
        assert hash1 == hash2

    def test_empty_string(self) -> None:
        """Empty string produces valid hash."""
        result = stable_hash_str("")
        assert len(result) == 64


class TestSeedFromStr:
    """Tests for seed_from_str function."""

    def test_deterministic(self) -> None:
        """Same string produces same seed."""
        seed1 = seed_from_str("my_table")
        seed2 = seed_from_str("my_table")
        assert seed1 == seed2

    def test_returns_32bit_int(self) -> None:
        """Returns a 32-bit unsigned integer."""
        seed = seed_from_str("test")
        assert isinstance(seed, int)
        assert 0 <= seed < 2**32

    def test_different_strings_different_seeds(self) -> None:
        """Different strings produce different seeds."""
        seed1 = seed_from_str("table_a")
        seed2 = seed_from_str("table_b")
        assert seed1 != seed2


class TestStableRowHash:
    """Tests for stable_row_hash function."""

    def test_deterministic(self) -> None:
        """Same row data produces same hash."""
        row = {"name": "Alice", "age": 30}
        hash1 = stable_row_hash(row)
        hash2 = stable_row_hash(row)
        assert hash1 == hash2

    def test_key_order_independent(self) -> None:
        """Key order does not affect hash."""
        row1 = {"name": "Alice", "age": 30}
        row2 = {"age": 30, "name": "Alice"}
        assert stable_row_hash(row1) == stable_row_hash(row2)

    def test_different_rows_different_hash(self) -> None:
        """Different rows produce different hashes."""
        row1 = {"name": "Alice", "age": 30}
        row2 = {"name": "Bob", "age": 30}
        assert stable_row_hash(row1) != stable_row_hash(row2)

import pytest

from dal.keyset_pagination import KeysetOrderKey, extract_keyset_order_keys


def test_extract_keyset_order_keys_basic():
    """Test standard extraction of columns from an ORDER BY clause."""
    sql = "SELECT id, name FROM users ORDER BY created_at DESC, id ASC"
    keys = extract_keyset_order_keys(sql)
    assert len(keys) == 2
    assert keys[0] == KeysetOrderKey("created_at", "created_at", True, True)  # DESC -> NULLS FIRST
    assert keys[1] == KeysetOrderKey("id", "id", False, False)  # ASC -> NULLS LAST


def test_extract_keyset_order_keys_no_order():
    """Test that queries without an ORDER BY clause return an empty list."""
    sql = "SELECT id FROM users"
    keys = extract_keyset_order_keys(sql)
    assert keys == []


def test_extract_keyset_order_keys_nondeterministic_random():
    """Test rejection of RANDOM() in ORDER BY."""
    sql = "SELECT id FROM users ORDER BY RANDOM()"
    with pytest.raises(ValueError, match="Nondeterministic ORDER BY expression"):
        extract_keyset_order_keys(sql)


def test_extract_keyset_order_keys_nondeterministic_uuid():
    """Test rejection of UUID generation in ORDER BY."""
    sql = "SELECT id FROM users ORDER BY gen_random_uuid()"
    with pytest.raises(ValueError, match="Nondeterministic ORDER BY expression"):
        extract_keyset_order_keys(sql)


def test_extract_keyset_order_keys_complex_expression():
    """Test extraction of complex expressions from ORDER BY."""
    sql = "SELECT id FROM users ORDER BY UPPER(name) ASC"
    keys = extract_keyset_order_keys(sql)
    assert len(keys) == 1
    assert keys[0].expression == "UPPER(name)"
    assert keys[0].descending is False


def test_extract_keyset_order_keys_invalid_sql():
    """Test that non-SELECT statements are rejected."""
    sql = "DELET FROM users"
    with pytest.raises(ValueError, match="Failed to parse SQL|Keyset pagination only supports"):
        extract_keyset_order_keys(sql)

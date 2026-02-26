import pytest

from dal.keyset_pagination import (
    KEYSET_ORDER_COLUMN_NOT_FOUND,
    KEYSET_REQUIRES_STABLE_TIEBREAKER,
    KEYSET_TIEBREAKER_NOT_UNIQUE,
    KEYSET_TIEBREAKER_NULLABLE,
    StaticSchemaInfoProvider,
    extract_keyset_order_keys,
    validate_stable_tiebreaker,
)


def test_extract_keyset_order_keys_basic():
    """Test standard extraction of columns from an ORDER BY clause."""
    sql = "SELECT id, name FROM users ORDER BY created_at DESC, id ASC"
    keys = extract_keyset_order_keys(sql)
    assert len(keys) == 2
    assert keys[0].expression.sql() == "created_at"
    assert keys[0].alias == "created_at"
    assert keys[0].descending is True
    assert keys[0].nulls_first is True  # DESC -> NULLS FIRST
    assert keys[1].expression.sql() == "id"
    assert keys[1].alias == "id"
    assert keys[1].descending is False
    assert keys[1].nulls_first is False  # ASC -> NULLS LAST


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
    assert keys[0].expression.sql() == "UPPER(name)"
    assert keys[0].descending is False


def test_extract_keyset_order_keys_postgres_defaults_null_ordering():
    """Postgres defaults should be DESC NULLS FIRST and ASC NULLS LAST."""
    sql = "SELECT id FROM users ORDER BY created_at DESC, id ASC"
    keys = extract_keyset_order_keys(sql, provider="postgres")
    assert keys[0].nulls_first is True
    assert keys[1].nulls_first is False


def test_extract_keyset_order_keys_non_postgres_without_nulls_clause_is_conservative():
    """Non-Postgres providers should avoid assuming provider-specific NULL defaults."""
    sql = "SELECT id FROM users ORDER BY created_at DESC"
    keys = extract_keyset_order_keys(sql, provider="mysql")
    assert len(keys) == 1
    assert keys[0].nulls_first is False


def test_extract_keyset_order_keys_invalid_sql():
    """Test that non-SELECT statements are rejected."""
    sql = "DELET FROM users"
    with pytest.raises(ValueError, match="Failed to parse SQL|Keyset pagination only supports"):
        extract_keyset_order_keys(sql)


def test_validate_stable_tiebreaker_rejects_created_at_only():
    """Single non-id tie-breakers should fail closed without metadata."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC")
    with pytest.raises(ValueError, match=KEYSET_REQUIRES_STABLE_TIEBREAKER):
        validate_stable_tiebreaker(keys, table_names=["users"])


def test_validate_stable_tiebreaker_allows_created_at_with_id():
    """Appending id as final tie-breaker is allowed without metadata."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC, id ASC")
    validate_stable_tiebreaker(keys, table_names=["users"], schema_info=None)


def test_validate_stable_tiebreaker_invokes_schema_info_provider_when_passed():
    """Schema-aware validation should consult provider methods for tie-breaker checks."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC, id ASC")

    class _Provider:
        def __init__(self) -> None:
            self.has_column_calls = 0
            self.is_nullable_calls = 0
            self.is_unique_key_calls = 0

        def has_column(self, table: str, col: str) -> bool:
            self.has_column_calls += 1
            return table == "users" and col in {"created_at", "id"}

        def is_nullable(self, table: str, col: str) -> bool | None:
            self.is_nullable_calls += 1
            return False

        def is_unique_key(self, table: str, col_set: list[str]) -> bool | None:
            self.is_unique_key_calls += 1
            return table == "users" and col_set == ["id"]

    provider = _Provider()
    validate_stable_tiebreaker(keys, table_names=["users"], schema_info=provider)
    assert provider.has_column_calls >= 2
    assert provider.is_nullable_calls == 1
    assert provider.is_unique_key_calls == 1


def test_validate_stable_tiebreaker_rejects_order_columns_missing_from_schema():
    """Schema-aware validation should reject ORDER BY keys not found in metadata."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC, id ASC")
    schema_info = StaticSchemaInfoProvider(
        by_table={
            "users": {
                "created_at": {"nullable": False},
            }
        }
    )
    with pytest.raises(ValueError, match=KEYSET_ORDER_COLUMN_NOT_FOUND):
        validate_stable_tiebreaker(keys, table_names=["users"], schema_info=schema_info)


def test_validate_stable_tiebreaker_rejects_nullable_tiebreaker_without_explicit_nulls():
    """Schema-aware validation should reject nullable final tie-breakers by default."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC, id ASC")
    schema_info = StaticSchemaInfoProvider(
        by_table={
            "users": {
                "created_at": {"nullable": False},
                "id": {"nullable": True, "is_primary_key": True},
            }
        }
    )
    with pytest.raises(ValueError, match=KEYSET_TIEBREAKER_NULLABLE):
        validate_stable_tiebreaker(keys, table_names=["users"], schema_info=schema_info)


def test_validate_stable_tiebreaker_allows_nullable_non_final_with_explicit_nulls_ordering():
    """Nullable non-final ORDER BY keys are allowed when NULL ordering is explicit."""
    keys = extract_keyset_order_keys(
        "SELECT id FROM users ORDER BY created_at DESC NULLS LAST, id ASC"
    )
    schema_info = StaticSchemaInfoProvider(
        by_table={
            "users": {
                "created_at": {"nullable": True},
                "id": {"nullable": False, "is_primary_key": True},
            }
        }
    )
    validate_stable_tiebreaker(keys, table_names=["users"], schema_info=schema_info)


def test_validate_stable_tiebreaker_allows_composite_unique_suffix():
    """Composite unique suffixes should satisfy keyset tie-breaker stability."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY user_id ASC, created_at ASC")
    schema_info = StaticSchemaInfoProvider(
        by_table={
            "users": {
                "user_id": {"nullable": False},
                "created_at": {"nullable": False},
            }
        },
        unique_keys_by_table={"users": [["user_id", "created_at"]]},
    )
    validate_stable_tiebreaker(keys, table_names=["users"], schema_info=schema_info)


def test_validate_stable_tiebreaker_rejects_non_unique_suffix_when_schema_knows_uniqueness():
    """Known uniqueness metadata should reject non-unique final tie-breakers."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at ASC")
    schema_info = StaticSchemaInfoProvider(
        by_table={
            "users": {
                "created_at": {"nullable": False},
                "id": {"nullable": False},
            }
        },
        unique_keys_by_table={"users": [["id"]]},
    )
    with pytest.raises(ValueError, match=KEYSET_TIEBREAKER_NOT_UNIQUE):
        validate_stable_tiebreaker(keys, table_names=["users"], schema_info=schema_info)


def test_validate_stable_tiebreaker_rejects_nullable_metadata_tiebreaker():
    """Metadata should reject nullable final tie-breaker columns."""
    keys = extract_keyset_order_keys("SELECT id FROM users ORDER BY created_at DESC, id ASC")
    with pytest.raises(ValueError, match=KEYSET_REQUIRES_STABLE_TIEBREAKER):
        validate_stable_tiebreaker(
            keys,
            table_names=["users"],
            column_metadata={
                "id": {
                    "nullable": True,
                    "is_primary_key": False,
                    "is_unique": False,
                }
            },
        )

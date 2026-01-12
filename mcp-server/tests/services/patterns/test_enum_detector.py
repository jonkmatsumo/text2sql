"""Unit tests for EnumLikeColumnDetector."""

from mcp_server.models import ColumnDef
from mcp_server.services.patterns.enum_detector import EnumLikeColumnDetector


def test_denylist_exclusion():
    """Test that denylist rules (wildcards and exact matches) function correctly."""
    detector = EnumLikeColumnDetector(denylist=["users.secret_code", "*.ignored"])

    # Matches explicit table.col
    col1 = ColumnDef(name="secret_code", data_type="text", is_nullable=True)
    assert not detector.is_candidate("users", col1)

    # Matches wildcard
    # Pattern '*.ignored' means any table, column exactly named "ignored"
    col2 = ColumnDef(name="ignored", data_type="text", is_nullable=True)
    assert not detector.is_candidate("orders", col2)

    # Test column wildcard
    # users.* -> Any column in users table
    detector3 = EnumLikeColumnDetector(denylist=["users.*"])
    col3 = ColumnDef(name="whatever", data_type="text", is_nullable=True)
    assert not detector3.is_candidate("users", col3)


def test_allowlist_priority():
    """Test that allowlist overrides PII/exclusion rules."""
    # Allowlist should override PII/ID heuristics
    detector = EnumLikeColumnDetector(allowlist=["users.email_status"])

    # 'email_status' triggers 'email' PII heuristic usually
    col = ColumnDef(name="email_status", data_type="text", is_nullable=True)

    # Without allowlist, it should be excluded
    detector_strict = EnumLikeColumnDetector()
    assert not detector_strict.is_candidate("users", col)

    # With allowlist, it should be included
    assert detector.is_candidate("users", col)


def test_pii_exclusion():
    """Test standard PII heuristics."""
    detector = EnumLikeColumnDetector()

    pii_names = [
        "user_email",
        "phone_number",
        "billing_address",
        "password_hash",
        "first_name",
    ]
    for name in pii_names:
        col = ColumnDef(name=name, data_type="text", is_nullable=True)
        assert not detector.is_candidate("users", col), f"Failed to exclude PII: {name}"


def test_id_exclusion():
    """Test ID column exclusion."""
    detector = EnumLikeColumnDetector()

    ids = ["id", "user_id", "order_id", "external_id"]
    for name in ids:
        col = ColumnDef(name=name, data_type="integer", is_nullable=True)
        assert not detector.is_candidate("users", col), f"Failed to exclude ID: {name}"


def test_type_exclusion():
    """Test data type exclusion."""
    detector = EnumLikeColumnDetector()

    types = ["json", "jsonb", "uuid", "blob", "bytea"]
    for dtype in types:
        col = ColumnDef(name="data", data_type=dtype, is_nullable=True)
        assert not detector.is_candidate("users", col), f"Failed to exclude type: {dtype}"


def test_canonicalization():
    """Test value canonicalization logic (trim, sort, etc.)."""
    detector = EnumLikeColumnDetector()

    raw = ["  apple ", "Banana", "apple", "", None, "  "]
    clean = detector.canonicalize_values(raw)

    # Should be sorted, deduped, trimmed, no empty/None
    assert clean == ["Banana", "apple"]

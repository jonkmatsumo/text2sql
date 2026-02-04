import pytest

from dal.util.logical_types import (
    logical_type_from_asyncpg_oid,
    logical_type_from_cursor_description,
    logical_type_from_db_type,
)


class TestLogicalTypeMapping:
    """Tests for logical type mapping utilities."""

    @pytest.mark.parametrize(
        "db_type,expected",
        [
            ("timestamptz", "timestamp"),
            ("timestamp", "timestamp"),
            ("date", "date"),
            ("time", "time"),
            ("boolean", "boolean"),
            ("uuid", "uuid"),
            ("jsonb", "json"),
            ("numeric(10,2)", "numeric"),
            ("decimal", "numeric"),
            ("float8", "float"),
            ("double precision", "float"),
            ("int4", "integer"),
            ("varchar", "string"),
            ("text", "string"),
        ],
    )
    def test_logical_type_from_db_type(self, db_type, expected):
        """Map db type strings to logical types."""
        assert logical_type_from_db_type(db_type) == expected

    @pytest.mark.parametrize(
        "oid,expected",
        [
            (16, "boolean"),
            (20, "integer"),
            (701, "float"),
            (1700, "numeric"),
            (1082, "date"),
            (1083, "time"),
            (1114, "timestamp"),
            (1184, "timestamp"),
            (114, "json"),
            (2950, "uuid"),
            (25, "string"),
            (999999, "unknown"),
        ],
    )
    def test_logical_type_from_asyncpg_oid(self, oid, expected):
        """Map asyncpg OIDs to logical types."""
        assert logical_type_from_asyncpg_oid(oid) == expected

    def test_logical_type_from_cursor_description_mysql_type_code(self):
        """Map mysql type codes from cursor description entries."""
        desc_entry = ("col", 3, None, None, None, None, None)
        assert logical_type_from_cursor_description(desc_entry, provider="mysql") == "integer"

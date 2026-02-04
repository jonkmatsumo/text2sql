from dataclasses import dataclass

from dal.util.column_metadata import (
    columns_from_asyncpg_attributes,
    columns_from_cursor_description,
)


@dataclass
class FakeType:
    """Test double for asyncpg type metadata."""

    name: str
    oid: int


@dataclass
class FakeAttr:
    """Test double for asyncpg attribute metadata."""

    name: str
    type: FakeType


class TestColumnMetadata:
    """Tests for column metadata utilities."""

    def test_columns_from_asyncpg_attributes(self):
        """Build column metadata from asyncpg-like attributes."""
        attrs = [
            FakeAttr(name="count", type=FakeType(name="int4", oid=23)),
            FakeAttr(name="created_at", type=FakeType(name="timestamptz", oid=1184)),
        ]
        columns = columns_from_asyncpg_attributes(attrs)
        assert columns == [
            {
                "name": "count",
                "type": "integer",
                "db_type": "int4",
                "nullable": None,
                "precision": None,
                "scale": None,
                "timezone": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "db_type": "timestamptz",
                "nullable": None,
                "precision": None,
                "scale": None,
                "timezone": None,
            },
        ]

    def test_columns_from_cursor_description_sqlite(self):
        """Build column metadata from sqlite cursor description."""
        description = [("id", None, None, None, None, None, None)]
        columns = columns_from_cursor_description(description, provider="sqlite")
        assert columns == [
            {
                "name": "id",
                "type": "unknown",
                "db_type": None,
                "nullable": None,
                "precision": None,
                "scale": None,
                "timezone": None,
            }
        ]

    def test_columns_from_cursor_description_mysql(self):
        """Build column metadata from mysql cursor description."""
        description = [("count", 3, None, None, None, None, None)]
        columns = columns_from_cursor_description(description, provider="mysql")
        assert columns[0]["name"] == "count"
        assert columns[0]["type"] == "integer"

from dataclasses import dataclass

from dal.util.column_metadata import columns_from_asyncpg_attributes


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

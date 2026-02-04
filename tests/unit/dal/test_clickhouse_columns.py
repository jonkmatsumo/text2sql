from dal.clickhouse.query_target import _columns_from_clickhouse_types


class TestClickHouseColumns:
    """Tests for ClickHouse column metadata helpers."""

    def test_columns_from_clickhouse_types(self):
        """Build columns from ClickHouse type tuples."""
        columns = _columns_from_clickhouse_types([("count", "Int64"), ("name", "String")])
        assert columns[0]["name"] == "count"
        assert columns[0]["type"] == "integer"
        assert columns[1]["type"] == "string"

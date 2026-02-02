import pytest

from dal.bigquery.schema_introspector import BigQuerySchemaIntrospector


@pytest.mark.asyncio
async def test_bigquery_schema_introspector(monkeypatch):
    """Validate BigQuery introspector mapping to canonical models."""

    class _FakeConfig:
        project = "proj"
        dataset = "dataset"
        location = None

    async def fake_run_query(sql, location, parameters=None):
        if "INFORMATION_SCHEMA.TABLES" in sql:
            return [{"table_name": "orders"}, {"table_name": "users"}]
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            assert parameters == {"table_name": "orders"}
            return [
                {"column_name": "id", "data_type": "INT64", "is_nullable": "NO"},
                {"column_name": "user_id", "data_type": "INT64", "is_nullable": "YES"},
            ]
        raise AssertionError(f"Unexpected SQL: {sql}")

    monkeypatch.setattr(
        "dal.bigquery.schema_introspector.BigQueryConfig.from_env",
        lambda: _FakeConfig(),
    )
    monkeypatch.setattr("dal.bigquery.schema_introspector._run_query", fake_run_query)

    introspector = BigQuerySchemaIntrospector()
    table_names = await introspector.list_table_names()
    assert table_names == ["orders", "users"]

    table_def = await introspector.get_table_def("orders")
    assert [col.name for col in table_def.columns] == ["id", "user_id"]
    assert [col.data_type for col in table_def.columns] == ["INT64", "INT64"]

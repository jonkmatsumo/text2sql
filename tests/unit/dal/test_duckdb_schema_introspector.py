"""Unit tests for DuckDB SchemaIntrospector."""

import pytest

from dal.duckdb.config import DuckDBConfig
from dal.duckdb.query_target import DuckDBQueryTargetDatabase

# Skip all tests if duckdb is not installed (optional dependency)
pytest.importorskip("duckdb")


@pytest.fixture
async def duckdb_with_schema(tmp_path):
    """Create a file-based DuckDB with test schema."""
    from dal.database import Database

    db_path = tmp_path / "test.duckdb"
    Database._query_target_provider = "duckdb"
    config = DuckDBConfig(
        path=str(db_path), query_timeout_seconds=30, max_rows=1000, read_only=False
    )
    await DuckDBQueryTargetDatabase.init(config)

    # Create test tables
    async with DuckDBQueryTargetDatabase.get_connection() as conn:
        await conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR)"
        )
        await conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))"
        )
        await conn.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        await conn.execute("INSERT INTO users VALUES (2, 'Bob', NULL)")
        await conn.execute("INSERT INTO orders VALUES (1, 1, 99.99)")

    yield

    await DuckDBQueryTargetDatabase.close()


@pytest.mark.asyncio
async def test_duckdb_list_table_names(duckdb_with_schema):
    """Verify list_table_names returns all user tables."""
    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()
    tables = await introspector.list_table_names()

    assert "users" in tables
    assert "orders" in tables
    assert len(tables) >= 2


@pytest.mark.asyncio
async def test_duckdb_get_table_def_columns(duckdb_with_schema):
    """Verify get_table_def returns correct columns."""
    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()
    table_def = await introspector.get_table_def("users")

    assert table_def.name == "users"
    assert len(table_def.columns) == 3

    col_names = [c.name for c in table_def.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names

    # Verify data types are populated
    for col in table_def.columns:
        assert col.data_type is not None
        assert col.data_type != ""


@pytest.mark.asyncio
async def test_duckdb_get_table_def_foreign_keys_empty(duckdb_with_schema):
    """The DuckDB introspector returns an empty foreign_keys list."""
    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()
    table_def = await introspector.get_table_def("orders")

    # DuckDB introspector does not extract FK metadata
    assert table_def.foreign_keys == []


@pytest.mark.asyncio
async def test_duckdb_get_sample_rows(duckdb_with_schema):
    """Verify get_sample_rows returns actual data."""
    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()
    rows = await introspector.get_sample_rows("users", limit=10)

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_duckdb_get_sample_rows_with_limit(duckdb_with_schema):
    """Verify get_sample_rows respects limit parameter."""
    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()
    rows = await introspector.get_sample_rows("users", limit=1)

    assert len(rows) == 1


async def test_duckdb_introspector_with_empty_table(tmp_path):
    """Verify introspection works on tables with no rows."""
    from dal.database import Database

    db_path = tmp_path / "empty.duckdb"
    Database._query_target_provider = "duckdb"
    config = DuckDBConfig(
        path=str(db_path), query_timeout_seconds=30, max_rows=1000, read_only=False
    )
    await DuckDBQueryTargetDatabase.init(config)

    async with DuckDBQueryTargetDatabase.get_connection() as conn:
        await conn.execute("CREATE TABLE empty_table (id INTEGER, value TEXT)")

    from dal.duckdb.schema_introspector import DuckDBSchemaIntrospector

    introspector = DuckDBSchemaIntrospector()

    # Should still return table definition
    table_def = await introspector.get_table_def("empty_table")
    assert table_def.name == "empty_table"
    assert len(table_def.columns) == 2

    # Sample rows should be empty
    rows = await introspector.get_sample_rows("empty_table")
    assert rows == []

    await DuckDBQueryTargetDatabase.close()

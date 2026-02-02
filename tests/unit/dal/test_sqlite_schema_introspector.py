import sqlite3

import pytest

from dal.database import Database
from dal.factory import reset_singletons


@pytest.mark.requires_db
@pytest.mark.asyncio
async def test_sqlite_schema_introspector_table_defs(tmp_path, monkeypatch):
    """Verify SQLite schema introspection returns tables, columns, and FKs."""
    # Reset factory singletons to ensure fresh state
    reset_singletons()

    db_path = tmp_path / "schema.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            amount REAL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("QUERY_TARGET_PROVIDER", "sqlite")
    monkeypatch.setenv("SQLITE_DB_PATH", str(db_path))
    # Disable control plane to avoid Postgres dependency
    monkeypatch.setenv("ENABLE_CONTROL_PLANE", "false")

    await Database.init()
    try:
        introspector = Database.get_schema_introspector()
        table_names = await introspector.list_table_names()
        assert table_names == ["orders", "users"]

        orders = await introspector.get_table_def("orders")
        assert [col.name for col in orders.columns] == ["id", "user_id", "amount"]
        assert orders.foreign_keys[0].column_name == "user_id"
        assert orders.foreign_keys[0].foreign_table_name == "users"
        assert orders.foreign_keys[0].foreign_column_name == "id"
    finally:
        await Database.close()

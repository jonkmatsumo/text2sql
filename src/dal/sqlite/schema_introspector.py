from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.database import Database
from schema import ColumnDef, ForeignKeyDef, TableDef


class SqliteSchemaIntrospector(SchemaIntrospector):
    """SQLite implementation of SchemaIntrospector using sqlite_master and PRAGMA."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List user-defined tables in the SQLite database."""
        _ = schema
        query = """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        return [row["name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Return the canonical table definition for a SQLite table."""
        _ = schema
        safe_table = table_name.replace('"', '""')
        cols_query = f'PRAGMA table_info("{safe_table}")'
        fk_query = f'PRAGMA foreign_key_list("{safe_table}")'

        async with Database.get_connection() as conn:
            col_rows = await conn.fetch(cols_query)
            fk_rows = await conn.fetch(fk_query)

        columns = [
            ColumnDef(
                name=row["name"],
                data_type=row["type"] or "UNKNOWN",
                is_nullable=(row["notnull"] == 0),
                is_primary_key=row["pk"] == 1,
            )
            for row in col_rows
        ]

        fks = [
            ForeignKeyDef(
                column_name=row["from"],
                foreign_table_name=row["table"],
                foreign_column_name=row["to"],
            )
            for row in fk_rows
        ]

        return TableDef(name=table_name, columns=columns, foreign_keys=fks, description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a SQLite table."""
        _ = schema
        safe_table = table_name.replace('"', '""')
        query = f'SELECT * FROM "{safe_table}" LIMIT ?'
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, limit)
        return rows

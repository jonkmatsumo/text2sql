from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.database import Database
from schema import ColumnDef, TableDef


class RedshiftSchemaIntrospector(SchemaIntrospector):
    """Redshift implementation of SchemaIntrospector using information_schema."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured Redshift schema."""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, schema)
        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns)."""
        cols_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """
        async with Database.get_connection() as conn:
            col_rows = await conn.fetch(cols_query, schema, table_name)

        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=(row["is_nullable"] == "YES"),
            )
            for row in col_rows
        ]

        return TableDef(name=table_name, columns=columns, foreign_keys=[], description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a Redshift table."""
        safe_schema = schema.replace('"', '""')
        safe_table = table_name.replace('"', '""')
        query = f'SELECT * FROM "{safe_schema}"."{safe_table}" LIMIT $1'
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, limit)
        return rows

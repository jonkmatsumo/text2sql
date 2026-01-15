from typing import List

from mcp_server.config.database import Database

from common.interfaces.schema_introspector import SchemaIntrospector
from schema import ColumnDef, ForeignKeyDef, TableDef


class PostgresSchemaIntrospector(SchemaIntrospector):
    """Postgres implementation of SchemaIntrospector using information_schema."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the specified schema."""
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, schema)

        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        async with Database.get_connection() as conn:
            # Columns
            cols_query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = $2
                ORDER BY ordinal_position
            """
            col_rows = await conn.fetch(cols_query, table_name, schema)

            # Foreign Keys
            fk_query = """
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = $1
                    AND tc.table_schema = $2
            """
            fk_rows = await conn.fetch(fk_query, table_name, schema)

            # Description (Comment)
            # Use pg_catalog logic as information_schema doesn't always expose comments easily
            desc_query = """
                SELECT obj_description(c.oid) as comment
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = $1 AND n.nspname = $2
            """
            desc_row = await conn.fetchrow(desc_query, table_name, schema)
            description = desc_row["comment"] if desc_row else None

        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=(row["is_nullable"] == "YES"),
            )
            for row in col_rows
        ]

        fks = [
            ForeignKeyDef(
                column_name=row["column_name"],
                foreign_table_name=row["foreign_table_name"],
                foreign_column_name=row["foreign_column_name"],
            )
            for row in fk_rows
        ]
        return TableDef(name=table_name, columns=columns, foreign_keys=fks, description=description)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows."""
        async with Database.get_connection() as conn:
            # Safe quoting
            safe_schema = schema.replace('"', '""')
            safe_table = table_name.replace('"', '""')
            query = f'SELECT * FROM "{safe_schema}"."{safe_table}" LIMIT $1'
            rows = await conn.fetch(query, limit)
            # Convert Record to dict and handle non-serializable types if necessary
            # For now, simplistic dict conversion (asyncpg Record is like a dict)
            return [dict(row) for row in rows]

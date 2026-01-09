from typing import List

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.schema_introspector import SchemaIntrospector
from mcp_server.models import ColumnDef, ForeignKeyDef, TableDef


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
        return TableDef(name=table_name, columns=columns, foreign_keys=fks)

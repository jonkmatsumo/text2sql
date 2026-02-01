from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.database import Database
from schema import ColumnDef, ForeignKeyDef, TableDef


class MysqlSchemaIntrospector(SchemaIntrospector):
    """MySQL implementation of SchemaIntrospector using information_schema."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the current MySQL database."""
        _ = schema
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        _ = schema
        async with Database.get_connection() as conn:
            cols_query = """
                SELECT column_name, data_type, column_type, is_nullable, column_key
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                AND table_name = %s
                ORDER BY ordinal_position
            """
            col_rows = await conn.fetch(cols_query, table_name)

            fk_query = """
                SELECT
                    column_name,
                    referenced_table_name AS foreign_table_name,
                    referenced_column_name AS foreign_column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = DATABASE()
                AND table_name = %s
                AND referenced_table_name IS NOT NULL
            """
            fk_rows = await conn.fetch(fk_query, table_name)

        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=_normalize_mysql_type(row["data_type"], row["column_type"]),
                is_nullable=(row["is_nullable"] == "YES"),
                is_primary_key=(row["column_key"] == "PRI"),
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

        return TableDef(name=table_name, columns=columns, foreign_keys=fks, description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a MySQL table."""
        _ = schema
        safe_table = table_name.replace("`", "``")
        query = f"SELECT * FROM `{safe_table}` LIMIT %s"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, limit)
        return rows


def _normalize_mysql_type(data_type: str, column_type: str) -> str:
    base = (data_type or "").lower()
    column = (column_type or "").lower()

    if base in {"tinyint"} and column.startswith("tinyint(1)"):
        return "bool"

    if base in {"bool", "boolean"}:
        return "bool"
    if base in {"int", "integer", "tinyint", "smallint", "mediumint", "bigint"}:
        return "int"
    if base in {"float", "double", "real"}:
        return "float"
    if base in {"decimal", "numeric"}:
        return "decimal"
    if base in {"varchar", "char", "text", "tinytext", "mediumtext", "longtext"}:
        return "text"
    if base in {"datetime", "timestamp"}:
        return "datetime"
    if base == "date":
        return "date"
    if base == "time":
        return "time"
    if base == "json":
        return "json"
    return base or "unknown"

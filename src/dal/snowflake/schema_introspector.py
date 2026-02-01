from typing import List

from common.config.env import get_env_str
from common.interfaces.schema_introspector import SchemaIntrospector
from dal.database import Database
from schema import ColumnDef, ForeignKeyDef, TableDef


class SnowflakeSchemaIntrospector(SchemaIntrospector):
    """Snowflake implementation of SchemaIntrospector using INFORMATION_SCHEMA."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured Snowflake schema."""
        database = _get_database()
        schema_name = _get_schema(schema)
        query = (
            f"SELECT table_name FROM {_info_schema_table(database, 'TABLES')} "
            "WHERE table_schema = $1 AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, schema_name)
        return [_get_row_value(row, "table_name") for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        database = _get_database()
        schema_name = _get_schema(schema)

        cols_query = (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM {_info_schema_table(database, 'COLUMNS')} "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position"
        )

        fk_query = (
            "SELECT "
            "  kcu.column_name, "
            "  pk.table_name AS referenced_table_name, "
            "  pk.column_name AS referenced_column_name "
            f"FROM {_info_schema_table(database, 'REFERENTIAL_CONSTRAINTS')} rc "
            f"JOIN {_info_schema_table(database, 'KEY_COLUMN_USAGE')} kcu "
            "  ON rc.constraint_name = kcu.constraint_name "
            " AND rc.constraint_schema = kcu.constraint_schema "
            f"JOIN {_info_schema_table(database, 'KEY_COLUMN_USAGE')} pk "
            "  ON rc.unique_constraint_name = pk.constraint_name "
            " AND rc.unique_constraint_schema = pk.constraint_schema "
            " AND kcu.ordinal_position = pk.ordinal_position "
            "WHERE kcu.table_schema = $1 AND kcu.table_name = $2"
        )

        async with Database.get_connection() as conn:
            col_rows = await conn.fetch(cols_query, schema_name, table_name)
            fk_rows = await conn.fetch(fk_query, schema_name, table_name)

        columns = [
            ColumnDef(
                name=_get_row_value(row, "column_name"),
                data_type=_get_row_value(row, "data_type"),
                is_nullable=(_get_row_value(row, "is_nullable") == "YES"),
            )
            for row in col_rows
        ]

        fks = [
            ForeignKeyDef(
                column_name=_get_row_value(row, "column_name"),
                foreign_table_name=_get_row_value(row, "referenced_table_name"),
                foreign_column_name=_get_row_value(row, "referenced_column_name"),
            )
            for row in fk_rows
        ]

        return TableDef(name=table_name, columns=columns, foreign_keys=fks, description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a Snowflake table."""
        database = _get_database()
        schema_name = _get_schema(schema)
        query = f"SELECT * FROM {_qualify_table(database, schema_name, table_name)} LIMIT $1"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, limit)
        return rows


def _get_database() -> str:
    return get_env_str("SNOWFLAKE_DATABASE", "")


def _get_schema(schema: str) -> str:
    configured = get_env_str("SNOWFLAKE_SCHEMA")
    return configured or schema


def _quote_ident(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _info_schema_table(database: str, table: str) -> str:
    return f"{_quote_ident(database)}.INFORMATION_SCHEMA.{table}"


def _qualify_table(database: str, schema: str, table: str) -> str:
    return f"{_quote_ident(database)}.{_quote_ident(schema)}.{_quote_ident(table)}"


def _get_row_value(row: dict, key: str) -> str:
    if key in row:
        return row[key]
    lower = key.lower()
    if lower in row:
        return row[lower]
    upper = key.upper()
    if upper in row:
        return row[upper]
    return row.get(key)

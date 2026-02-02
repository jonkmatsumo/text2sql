from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.clickhouse.config import ClickHouseConfig
from dal.database import Database
from schema import ColumnDef, TableDef


class ClickHouseSchemaIntrospector(SchemaIntrospector):
    """ClickHouse implementation of SchemaIntrospector using system tables."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured ClickHouse database."""
        config = ClickHouseConfig.from_env()
        query = """
            SELECT name
            FROM system.tables
            WHERE database = {db: String} AND is_temporary = 0
            ORDER BY name
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, config.database)
        return [row["name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns)."""
        config = ClickHouseConfig.from_env()
        query = """
            SELECT name, type, is_in_primary_key
            FROM system.columns
            WHERE database = {db: String} AND table = {table: String}
            ORDER BY position
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, config.database, table_name)

        columns = [
            ColumnDef(
                name=row["name"],
                data_type=row["type"],
                is_nullable=True,
            )
            for row in rows
        ]
        return TableDef(name=table_name, columns=columns, foreign_keys=[], description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a ClickHouse table."""
        config = ClickHouseConfig.from_env()
        query = f"SELECT * FROM {config.database}.{table_name} LIMIT {int(limit)}"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        return rows

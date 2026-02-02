from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.database import Database
from dal.databricks.config import DatabricksConfig
from schema import ColumnDef, TableDef


class DatabricksSchemaIntrospector(SchemaIntrospector):
    """Databricks UC implementation of SchemaIntrospector."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured Databricks catalog/schema."""
        config = DatabricksConfig.from_env()
        query = """
            SELECT table_name
            FROM system.information_schema.tables
            WHERE table_catalog = $1 AND table_schema = $2 AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, config.catalog, config.schema)
        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns)."""
        config = DatabricksConfig.from_env()
        query = """
            SELECT column_name, data_type, is_nullable
            FROM system.information_schema.columns
            WHERE table_catalog = $1 AND table_schema = $2 AND table_name = $3
            ORDER BY ordinal_position
        """
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query, config.catalog, config.schema, table_name)

        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=(row["is_nullable"] == "YES"),
            )
            for row in rows
        ]
        return TableDef(name=table_name, columns=columns, foreign_keys=[], description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a Databricks table."""
        config = DatabricksConfig.from_env()
        query = f"SELECT * FROM {config.catalog}.{config.schema}.{table_name} LIMIT {int(limit)}"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        return rows

from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.athena.config import AthenaConfig
from dal.database import Database
from schema import ColumnDef, TableDef


class AthenaSchemaIntrospector(SchemaIntrospector):
    """Athena implementation of SchemaIntrospector using SHOW/DESCRIBE."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured Athena database."""
        config = AthenaConfig.from_env()
        query = f"SHOW TABLES IN {config.database}"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        table_names = []
        for row in rows:
            table_names.append(row.get("tab_name") or row.get("table_name"))
        return [name for name in table_names if name]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns)."""
        config = AthenaConfig.from_env()
        query = f"DESCRIBE {config.database}.{table_name}"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)

        columns = []
        for row in rows:
            col_name = row.get("col_name") or row.get("column_name")
            data_type = row.get("data_type") or row.get("type")
            if not col_name or col_name.startswith("#"):
                continue
            columns.append(ColumnDef(name=col_name, data_type=data_type or "", is_nullable=True))

        return TableDef(name=table_name, columns=columns, foreign_keys=[], description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for an Athena table."""
        config = AthenaConfig.from_env()
        query = f"SELECT * FROM {config.database}.{table_name} LIMIT {int(limit)}"
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
        return rows

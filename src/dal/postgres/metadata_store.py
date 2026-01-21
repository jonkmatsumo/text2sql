import json
from typing import List

from common.interfaces.metadata_store import MetadataStore
from dal.postgres.schema_introspector import PostgresSchemaIntrospector


class PostgresMetadataStore(MetadataStore):
    """Postgres implementation of MetadataStore.

    Uses SchemaIntrospector to get structured data and formats it for tool output.
    """

    def __init__(self):
        """Initialize the metadata store with a schema introspector."""
        self._introspector = PostgresSchemaIntrospector()

    async def list_tables(self, schema: str = "public") -> List[str]:
        """List all available tables."""
        return await self._introspector.list_table_names(schema)

    async def get_table_definition(self, table_name: str) -> str:
        """Get the table schema formatted as the legacy tool expects (JSON)."""
        table_def = await self._introspector.get_table_def(table_name)

        # Format to match legacy.py output methodology
        columns_data = [
            {
                "name": col.name,
                "type": col.data_type,
                "nullable": col.is_nullable,
            }
            for col in table_def.columns
        ]

        foreign_keys = [
            {
                "column": fk.column_name,
                "foreign_table": fk.foreign_table_name,
                "foreign_column": fk.foreign_column_name,
            }
            for fk in table_def.foreign_keys
        ]

        definition = {
            "table_name": table_name,
            "columns": columns_data,
            "foreign_keys": foreign_keys,
        }

        # Return as JSON string just like the legacy tool did (but singular)
        return json.dumps(definition)

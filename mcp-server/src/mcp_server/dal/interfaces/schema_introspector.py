from typing import List, Protocol, runtime_checkable

from mcp_server.models.database.table_def import TableDef


@runtime_checkable
class SchemaIntrospector(Protocol):
    """Protocol for introspecting database schema (tables, columns, constraints)."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the specified schema."""
        ...

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        ...

from typing import List, Protocol, runtime_checkable

from schema import TableDef


@runtime_checkable
class SchemaIntrospector(Protocol):
    """Protocol for introspecting database schema (tables, columns, constraints)."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the specified schema."""
        ...

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns, FKs)."""
        ...

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Get sample rows for a specific table."""
        ...

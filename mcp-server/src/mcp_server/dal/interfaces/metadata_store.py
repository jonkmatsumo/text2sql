from typing import List, Protocol, runtime_checkable


@runtime_checkable
class MetadataStore(Protocol):
    """Protocol for high-level database metadata access (used by Agent Tools)."""

    async def list_tables(self, schema: str = "public") -> List[str]:
        """List all available tables."""
        ...

    async def get_table_definition(self, table_name: str) -> str:
        """Get a string representation of the table schema (DDL or JSON)."""
        ...

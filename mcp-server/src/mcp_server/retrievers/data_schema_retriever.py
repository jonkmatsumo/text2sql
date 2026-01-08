from abc import ABC, abstractmethod
from typing import Dict, List

from mcp_server.models.schema import ColumnMetadata, ForeignKey, TableMetadata


class DataSchemaRetriever(ABC):
    """Abstract base class for retrieving database schema metadata."""

    @abstractmethod
    def list_tables(self) -> List[TableMetadata]:
        """List all tables in the database with basic metadata."""
        pass

    @abstractmethod
    def get_columns(self, table_name: str) -> List[ColumnMetadata]:
        """Get detailed column information for a specific table."""
        pass

    @abstractmethod
    def get_foreign_keys(self, table_name: str) -> List[ForeignKey]:
        """Get foreign key relationships for a specific table."""
        pass

    @abstractmethod
    def get_sample_rows(self, table_name: str, limit: int = 3) -> List[Dict]:
        """Get sample rows for a specific table."""
        pass

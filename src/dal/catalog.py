from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CatalogNamespace:
    """Three-level namespace helper for catalog-backed warehouses."""

    level1: str
    level2: str
    table: Optional[str] = None

    def to_bigquery(self) -> str:
        """Format as project.dataset[.table] for BigQuery."""
        if self.table:
            return f"{self.level1}.{self.level2}.{self.table}"
        return f"{self.level1}.{self.level2}"

    def to_databricks(self) -> str:
        """Format as catalog.schema[.table] for Databricks."""
        if self.table:
            return f"{self.level1}.{self.level2}.{self.table}"
        return f"{self.level1}.{self.level2}"

    def to_snowflake(self) -> str:
        """Format as database.schema[.table] for Snowflake-style catalogs."""
        if self.table:
            return f"{self.level1}.{self.level2}.{self.table}"
        return f"{self.level1}.{self.level2}"

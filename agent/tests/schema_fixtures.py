from dataclasses import dataclass
from typing import List


@dataclass
class DatasetSchemaFixture:
    """Fixture defining schema-specific literals for tests."""

    name: str
    # Primary valid table name for simple queries
    valid_table: str
    # A table name guaranteed NOT to exist
    invalid_table: str
    # List of valid tables for whitelist mocking
    tables: List[str]
    # A valid simple SELECT query
    sample_query: str
    # A generic count query
    count_query: str


# Concrete implementation for Pagila (Legacy/Default)
PAGILA_FIXTURE = DatasetSchemaFixture(
    name="pagila",
    valid_table="film",
    invalid_table="non_existent_table",
    tables=["film", "actor", "customer", "rental", "payment"],
    sample_query="SELECT * FROM film LIMIT 1",
    count_query="SELECT COUNT(*) as count FROM film",
)

# Concrete implementation for Synthetic (New/Target)
SYNTHETIC_FIXTURE = DatasetSchemaFixture(
    name="synthetic",
    valid_table="dim_customer",
    invalid_table="non_existent_table",
    tables=["dim_customer", "fact_transaction", "dim_account", "dim_merchant"],
    sample_query="SELECT * FROM dim_customer LIMIT 1",
    count_query="SELECT COUNT(*) as count FROM dim_customer",
)

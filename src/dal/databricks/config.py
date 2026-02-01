from dataclasses import dataclass

from common.config.env import get_env_int, get_env_str


@dataclass(frozen=True)
class DatabricksConfig:
    """Configuration required for Databricks SQL Warehouse access."""

    host: str
    token: str
    warehouse_id: str
    catalog: str
    schema: str
    query_timeout_seconds: int
    poll_interval_seconds: int
    max_rows: int

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Load Databricks config from environment variables."""
        host = get_env_str("DATABRICKS_HOST")
        token = get_env_str("DATABRICKS_TOKEN")
        warehouse_id = get_env_str("DATABRICKS_WAREHOUSE_ID")
        catalog = get_env_str("DATABRICKS_CATALOG")
        schema = get_env_str("DATABRICKS_SCHEMA")
        query_timeout_seconds = get_env_int("DATABRICKS_QUERY_TIMEOUT_SECS", 30)
        poll_interval_seconds = get_env_int("DATABRICKS_POLL_INTERVAL_SECS", 1)
        max_rows = get_env_int("DATABRICKS_MAX_ROWS", 1000)

        missing = [
            name
            for name, value in {
                "DATABRICKS_HOST": host,
                "DATABRICKS_TOKEN": token,
                "DATABRICKS_WAREHOUSE_ID": warehouse_id,
                "DATABRICKS_CATALOG": catalog,
                "DATABRICKS_SCHEMA": schema,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Databricks query target missing required config: {missing_list}. "
                "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID, "
                "DATABRICKS_CATALOG, and DATABRICKS_SCHEMA."
            )

        return cls(
            host=host,
            token=token,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema,
            query_timeout_seconds=query_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            max_rows=max_rows,
        )

from dataclasses import dataclass
from typing import Optional

from common.config.env import get_env_int, get_env_str


@dataclass(frozen=True)
class SnowflakeConfig:
    """Configuration required for Snowflake query-target access."""

    account: str
    user: str
    password: Optional[str]
    warehouse: str
    database: str
    schema: str
    role: Optional[str]
    authenticator: Optional[str]
    query_timeout_seconds: int
    poll_interval_seconds: int
    max_rows: int
    warn_after_seconds: int

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load Snowflake config from environment variables."""
        account = get_env_str("SNOWFLAKE_ACCOUNT")
        user = get_env_str("SNOWFLAKE_USER")
        password = get_env_str("SNOWFLAKE_PASSWORD")
        warehouse = get_env_str("SNOWFLAKE_WAREHOUSE")
        database = get_env_str("SNOWFLAKE_DATABASE")
        schema = get_env_str("SNOWFLAKE_SCHEMA")
        role = get_env_str("SNOWFLAKE_ROLE")
        authenticator = get_env_str("SNOWFLAKE_AUTHENTICATOR")
        query_timeout_seconds = get_env_int("SNOWFLAKE_QUERY_TIMEOUT_SECS", 30)
        poll_interval_seconds = get_env_int("SNOWFLAKE_POLL_INTERVAL_SECS", 1)
        max_rows = get_env_int("SNOWFLAKE_MAX_ROWS", 1000)
        warn_after_seconds = get_env_int("SNOWFLAKE_WARN_AFTER_SECS", 10)

        missing = [
            name
            for name, value in {
                "SNOWFLAKE_ACCOUNT": account,
                "SNOWFLAKE_USER": user,
                "SNOWFLAKE_WAREHOUSE": warehouse,
                "SNOWFLAKE_DATABASE": database,
                "SNOWFLAKE_SCHEMA": schema,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Snowflake query target missing required config: {missing_list}. "
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_WAREHOUSE, "
                "SNOWFLAKE_DATABASE, and SNOWFLAKE_SCHEMA."
            )

        return cls(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            authenticator=authenticator,
            query_timeout_seconds=query_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            max_rows=max_rows,
            warn_after_seconds=warn_after_seconds,
        )

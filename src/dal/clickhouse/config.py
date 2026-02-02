from dataclasses import dataclass

from common.config.env import get_env_int, get_env_str


@dataclass(frozen=True)
class ClickHouseConfig:
    """Configuration required for ClickHouse query-target access."""

    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool
    query_timeout_seconds: int
    max_rows: int

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Load ClickHouse config from environment variables."""
        host = get_env_str("CLICKHOUSE_HOST")
        port = get_env_int("CLICKHOUSE_PORT", 9000)
        database = get_env_str("CLICKHOUSE_DB")
        user = get_env_str("CLICKHOUSE_USER", "default")
        password = get_env_str("CLICKHOUSE_PASS", "")
        secure = get_env_str("CLICKHOUSE_SECURE", "false").lower() == "true"
        query_timeout_seconds = get_env_int("CLICKHOUSE_QUERY_TIMEOUT_SECS", 30)
        max_rows = get_env_int("CLICKHOUSE_MAX_ROWS", 1000)

        missing = [
            name
            for name, value in {
                "CLICKHOUSE_HOST": host,
                "CLICKHOUSE_DB": database,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"ClickHouse query target missing required config: {missing_list}. "
                "Set CLICKHOUSE_HOST and CLICKHOUSE_DB."
            )

        return cls(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            secure=secure,
            query_timeout_seconds=query_timeout_seconds,
            max_rows=max_rows,
        )

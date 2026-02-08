from dataclasses import dataclass

from common.config.env import get_env_bool, get_env_int, get_env_str


@dataclass(frozen=True)
class DuckDBConfig:
    """Configuration required for DuckDB query-target access."""

    path: str
    query_timeout_seconds: int
    max_rows: int
    read_only: bool = True

    @classmethod
    def from_env(cls) -> "DuckDBConfig":
        """Load DuckDB config from environment variables."""
        path = get_env_str("DUCKDB_PATH", ":memory:")
        query_timeout_seconds = get_env_int("DUCKDB_QUERY_TIMEOUT_SECS", 30)
        max_rows = get_env_int("DUCKDB_MAX_ROWS", 1000)
        read_only = get_env_bool("DUCKDB_READ_ONLY", True)
        return cls(
            path=path,
            query_timeout_seconds=query_timeout_seconds,
            max_rows=max_rows,
            read_only=read_only,
        )

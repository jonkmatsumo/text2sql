from dataclasses import dataclass
from typing import Optional

from common.config.env import get_env_int, get_env_str


@dataclass(frozen=True)
class BigQueryConfig:
    """Configuration required for BigQuery query-target access."""

    project: str
    dataset: str
    location: Optional[str]
    query_timeout_seconds: int
    poll_interval_seconds: int
    max_rows: int

    @classmethod
    def from_env(cls) -> "BigQueryConfig":
        """Load BigQuery config from environment variables."""
        project = get_env_str("BIGQUERY_PROJECT")
        dataset = get_env_str("BIGQUERY_DATASET")
        location = get_env_str("BIGQUERY_LOCATION")
        query_timeout_seconds = get_env_int("BIGQUERY_QUERY_TIMEOUT_SECS", 30)
        poll_interval_seconds = get_env_int("BIGQUERY_POLL_INTERVAL_SECS", 1)
        max_rows = get_env_int("BIGQUERY_MAX_ROWS", 1000)

        missing = [
            name
            for name, value in {
                "BIGQUERY_PROJECT": project,
                "BIGQUERY_DATASET": dataset,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"BigQuery query target missing required config: {missing_list}. "
                "Set BIGQUERY_PROJECT and BIGQUERY_DATASET."
            )

        return cls(
            project=project,
            dataset=dataset,
            location=location,
            query_timeout_seconds=query_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            max_rows=max_rows,
        )

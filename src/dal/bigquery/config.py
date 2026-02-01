from dataclasses import dataclass
from typing import Optional

from common.config.env import get_env_str


@dataclass(frozen=True)
class BigQueryConfig:
    """Configuration required for BigQuery query-target access."""

    project: str
    dataset: str
    location: Optional[str]

    @classmethod
    def from_env(cls) -> "BigQueryConfig":
        """Load BigQuery config from environment variables."""
        project = get_env_str("BIGQUERY_PROJECT")
        dataset = get_env_str("BIGQUERY_DATASET")
        location = get_env_str("BIGQUERY_LOCATION")

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

        return cls(project=project, dataset=dataset, location=location)

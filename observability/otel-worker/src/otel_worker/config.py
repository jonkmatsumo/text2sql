from typing import Optional

from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration settings for the OTEL worker."""

    POSTGRES_URL: Optional[str] = None
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "password"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "postgres"

    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "otel-traces"

    MLFLOW_TRACKING_URI: Optional[str] = None
    ENABLE_MLFLOW_EXPORT: bool = True
    OTEL_ENVIRONMENT: str = "local"

    BATCH_MAX_SIZE: int = 25
    BATCH_FLUSH_INTERVAL_MS: int = 200

    QUEUE_MAX_DEPTH: int = 1000
    OVERFLOW_POLICY: str = "drop"  # drop, reject, sample
    OVERFLOW_SAMPLE_RATE: float = 0.1

    @model_validator(mode="after")
    def build_postgres_url(self) -> "Settings":
        """Build POSTGRES_URL if not provided or if it's a dummy value."""
        # Force dummy markers to match both cases and a variety of placeholders
        dummy_markers = ["user:pass", "driver://", "dbname", "postgresql://user:pass"]
        current_url = str(self.POSTGRES_URL or "").lower()
        is_dummy = any(m in current_url for m in dummy_markers)

        if not self.POSTGRES_URL or is_dummy:
            # We use individual fields which should be populated by load_dotenv or environment
            self.POSTGRES_URL = (
                f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
                f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
            )
        return self

    class Config:
        """Pydantic config."""

        case_sensitive = True


settings = Settings()

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Configuration settings for the OTEL worker."""

    POSTGRES_URL: str
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET: str = "otel-traces"
    MLFLOW_TRACKING_URI: str
    ENABLE_MLFLOW_EXPORT: bool = True
    OTEL_ENVIRONMENT: str = "local"

    class Config:
        """Pydantic config."""

        case_sensitive = True


settings = Settings()

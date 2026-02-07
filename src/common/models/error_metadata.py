"""Structured error metadata models."""

from typing import Optional

from pydantic import BaseModel, Field


class ErrorMetadata(BaseModel):
    """Structured metadata for database and application errors."""

    sql_state: Optional[str] = Field(
        None, description="Database-specific error code (e.g. SQLSTATE)"
    )
    message: str = Field(
        ..., max_length=2048, description="The error message, potentially redacted"
    )
    line_number: Optional[int] = Field(None, description="Line number where the error occurred")
    position: Optional[int] = Field(None, description="Character position where the error occurred")
    hint: Optional[str] = Field(
        None, max_length=2048, description="Provider-specific hint or suggestion"
    )
    provider: str = Field(..., description="The name of the database provider")
    category: str = Field(..., description="Provider-agnostic error category")
    is_retryable: bool = Field(..., description="Whether the error is considered retryable")
    retry_after_seconds: Optional[float] = Field(
        None, description="Suggested delay before retrying"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses/telemetry."""
        return self.model_dump(exclude_none=True)

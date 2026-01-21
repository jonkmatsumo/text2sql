"""Evaluation domain models for control-plane persistence."""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class EvaluationRunCreate(BaseModel):
    """Payload for creating a new evaluation run."""

    dataset_mode: str
    dataset_version: Optional[str] = None
    git_sha: Optional[str] = None
    tenant_id: int = 1
    config_snapshot: Dict[str, Any] = Field(default_factory=dict)


class EvaluationRun(EvaluationRunCreate):
    """Full evaluation run record."""

    id: str  # run_id (string or UUID string)
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: str = "RUNNING"
    metrics_summary: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class EvaluationCaseResultCreate(BaseModel):
    """Payload for saving a single test case result."""

    run_id: str
    test_id: str
    question: str
    generated_sql: Optional[str] = None
    is_correct: bool
    structural_score: float
    error_message: Optional[str] = None
    execution_time_ms: int
    raw_response: Dict[str, Any] = Field(default_factory=dict)
    trace_id: Optional[str] = None


class EvaluationCaseResult(EvaluationCaseResultCreate):
    """Full evaluation result record."""

    id: UUID
    created_at: datetime

import time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EvaluationConfig(BaseModel):
    """Configuration for an evaluation run."""

    dataset_path: str = Field(..., description="Path to the golden dataset JSONL file")
    output_dir: str = Field(..., description="Directory to store evaluation artifacts")
    run_id: Optional[str] = Field(
        None, description="Unique identifier for the run. Defaults to timestamp."
    )

    # Execution parameters
    concurrency: int = Field(1, description="Number of concurrent evaluations")
    limit: Optional[int] = Field(
        None, description="Limit the number of cases to run (for debugging)"
    )
    seed: int = Field(42, description="Random seed for determinism")

    # Target configuration
    tenant_id: int = Field(1, description="Tenant ID to use for evaluation")

    # Metadata
    git_sha: Optional[str] = Field(None, description="Git SHA of the code being evaluated")
    agent_config: Dict[str, Any] = Field(
        default_factory=dict, description="Configuration overrides for the agent"
    )


class EvaluationCaseResult(BaseModel):
    """Result of a single evaluation case."""

    case_id: str
    question: str
    expected_sql: Optional[str]
    generated_sql: Optional[str]
    is_correct: bool
    execution_status: str  # SUCCESS, FAILURE, CLARIFICATION_REQUIRED
    error: Optional[str] = None
    latency_ms: float

    # Detailed telemetry/debug info
    trace_id: Optional[str] = None
    steps_taken: List[str] = Field(default_factory=list)


class EvaluationSummary(BaseModel):
    """Aggregated metrics for the evaluation run."""

    run_id: str
    timestamp: float = Field(default_factory=time.time)
    config: EvaluationConfig

    # High-level metrics
    total_cases: int
    successful_cases: int
    failed_cases: int
    accuracy: float
    avg_latency_ms: float
    p95_latency_ms: float

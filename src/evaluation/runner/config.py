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

    # Metrics V1 Fields
    exact_match: bool = False
    structural_score: float = 0.0
    subscores: Dict[str, float] = Field(default_factory=dict)
    generated_tables: List[str] = Field(default_factory=list)
    expected_tables: List[str] = Field(default_factory=list)
    parse_errors: List[str] = Field(default_factory=list)

    # Deprecated fields (use exact_match instead of is_correct)
    is_correct: bool  # @deprecated: use exact_match

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

    # Metrics V1 Aggregations
    exact_match_count: int = 0
    exact_match_rate: float = 0.0
    avg_structural_score: float = 0.0
    min_structural_score: float = 0.0
    dataset_source: Optional[str] = None

    # High-level metrics
    total_cases: int

    # Deprecated fields
    successful_cases: int  # @deprecated: use exact_match_count
    failed_cases: int  # @deprecated: calculate from total_cases - exact_match_count
    accuracy: float  # @deprecated: use exact_match_rate

    avg_latency_ms: float
    p95_latency_ms: float

import time
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from .config import EvaluationConfig


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
    # Metrics V2 Fields
    structural_score_v2: Optional[float] = None
    value_aware_score: Optional[float] = None
    v2_subscores: Optional[Dict[str, float]] = None
    metrics_version: str = "v1"

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
    # Metrics V2 Aggregations
    avg_structural_score_v2: Optional[float] = None
    metrics_version: str = "v1"

    # High-level metrics
    total_cases: int

    # Deprecated fields
    successful_cases: int  # @deprecated: use exact_match_count
    failed_cases: int  # @deprecated: calculate from total_cases - exact_match_count
    accuracy: float  # @deprecated: use exact_match_rate

    avg_latency_ms: float
    p95_latency_ms: float

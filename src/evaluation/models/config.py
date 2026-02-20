from typing import Any, Dict, Literal, Optional

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
    metrics_version: Literal["v1", "v2"] = Field("v1", description="Metrics suite version to use")

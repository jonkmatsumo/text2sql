"""Standardized decision summary utilities."""

import time
from typing import Any, Dict, Union

from pydantic import BaseModel, Field


class DecisionSummary(BaseModel):
    """Machine-readable summary of a system decision (retry, pagination, etc.)."""

    action: str = Field(
        ..., description="The action being decided (e.g. 'retry', 'pagination', 'prefetch')"
    )
    decision: str = Field(
        ..., description="The outcome of the decision (e.g. 'proceed', 'stop', 'suppress')"
    )
    reason_code: str = Field(..., description="Canonical reason code for the decision")
    timestamp: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Action-specific metadata")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for telemetry/logging."""
        return self.model_dump(exclude_none=True)


def format_decision_summary(
    action: str, decision: str, reason_code: Union[str, Any], **metadata: Any
) -> DecisionSummary:
    """Create a standardized DecisionSummary."""
    # Handle Enum objects by taking their value
    if hasattr(reason_code, "value"):
        reason_code = reason_code.value

    return DecisionSummary(
        action=action, decision=decision, reason_code=str(reason_code), metadata=metadata
    )

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SelectionSummary(BaseModel):
    """Summary of candidate counts at different stages of selection."""

    total_candidates: int = 0
    returned_count: int = 0
    counts_by_source: Dict[str, int] = Field(default_factory=dict)
    counts_by_status: Dict[str, int] = Field(default_factory=dict)


class FilteringExplanation(BaseModel):
    """Explanation of candidates removed during validity and safety filtering."""

    tombstoned_removed: int = 0
    missing_fields_removed: int = 0
    stale_removed: int = 0
    safety_removed: int = 0  # Placeholder for #119


class DiversityEffects(BaseModel):
    """Specific effects of applied diversity policies."""

    verified_floor_applied: bool = False
    source_caps_applied: Dict[str, int] = Field(default_factory=dict)


class DiversityExplanation(BaseModel):
    """Explanation of diversity policy application and its effects."""

    enabled: bool = False
    min_verified: int = 0
    max_per_source: int = -1
    applied: bool = False
    effects: DiversityEffects = Field(default_factory=DiversityEffects)


class PinsExplanation(BaseModel):
    """Explanation of pin rule matching and application."""

    enabled: bool = True
    matched_rules: List[str] = Field(default_factory=list)
    selected_count: int = 0
    applied_before_ranking: bool = True


class FallbackExplanation(BaseModel):
    """Explanation of fallback usage when primary results are insufficient."""

    enabled: bool = False
    used: bool = False
    reason: Optional[str] = None
    candidate_multiplier: int = 1
    shortage_count: int = 0


class RecommendationExplanation(BaseModel):
    """Root model for the recommendation ranking breakdown."""

    selection_summary: SelectionSummary = Field(default_factory=SelectionSummary)
    filtering: FilteringExplanation = Field(default_factory=FilteringExplanation)
    diversity: DiversityExplanation = Field(default_factory=DiversityExplanation)
    pins: PinsExplanation = Field(default_factory=PinsExplanation)
    fallback: FallbackExplanation = Field(default_factory=FallbackExplanation)

    def to_dict(self) -> dict:
        """Convert the model to a dictionary."""
        return self.model_dump()

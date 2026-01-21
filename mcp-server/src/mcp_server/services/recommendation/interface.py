from typing import List, Optional

from pydantic import BaseModel, Field

from mcp_server.services.recommendation.explanation import RecommendationExplanation


class RecommendedExample(BaseModel):
    """A recommended few-shot example."""

    question: str
    sql: str
    score: float
    source: str  # 'approved', 'seeded', or 'fallback'
    canonical_group_id: Optional[str] = None
    metadata: dict = Field(default_factory=dict)


class RecommendationResult(BaseModel):
    """Result from the recommendation service."""

    examples: List[RecommendedExample]
    fallback_used: bool = False
    metadata: dict = Field(default_factory=dict)
    explanation: Optional[RecommendationExplanation] = None

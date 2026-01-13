from mcp_server.services.recommendation.config import RECO_CONFIG
from mcp_server.services.recommendation.service import RecommendationService


async def handler(query: str, tenant_id: int = 1, limit: int = RECO_CONFIG.limit_default) -> dict:
    """Recommend few-shot examples for a given natural language query.

    Args:
        query: The user's natural language question.
        tenant_id: Tenant identifier.
        limit: Maximum number of examples to recommend.

    Returns:
        JSON compatible dictionary with recommended examples and fallback status.
    """
    result = await RecommendationService.recommend_examples(
        question=query, tenant_id=tenant_id, limit=limit
    )

    return result.model_dump()

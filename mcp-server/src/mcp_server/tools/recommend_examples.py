from typing import Optional

from mcp_server.services.recommendation.config import RECO_CONFIG
from mcp_server.services.recommendation.service import RecommendationService


async def handler(
    query: str,
    tenant_id: int = 1,
    limit: int = RECO_CONFIG.limit_default,
    enable_fallback: Optional[bool] = None,
) -> dict:
    """Recommend few-shot examples for a given natural language query.

    Args:
        query: The user's natural language question.
        tenant_id: Tenant identifier.
        limit: Maximum number of examples to recommend.
        enable_fallback: Whether to search for interactions if few-shots are insufficient.
            If None, uses service default (True).

    Returns:
        JSON compatible dictionary with recommended examples and fallback status.
    """
    # Build kwargs to avoid passing None if we want to rely on service defaults,
    # though recommend_examples has a default of True.
    kwargs = {"question": query, "tenant_id": tenant_id, "limit": limit}
    if enable_fallback is not None:
        kwargs["enable_fallback"] = enable_fallback

    result = await RecommendationService.recommend_examples(**kwargs)

    return result.model_dump()

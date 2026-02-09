from typing import Optional

from mcp_server.services.recommendation.config import RECO_CONFIG
from mcp_server.services.recommendation.service import RecommendationService

TOOL_NAME = "recommend_examples"


async def handler(
    query: str,
    tenant_id: int,
    limit: int = RECO_CONFIG.limit_default,
    enable_fallback: Optional[bool] = None,
) -> str:
    """Recommend few-shot examples for a given natural language query.

    Args:
        query: The user's natural language question.
        tenant_id: Tenant identifier (REQUIRED).
        limit: Maximum number of examples to recommend.
        enable_fallback: Whether to search for interactions if few-shots are insufficient.
            If None, uses service default (True).

    Returns:
        JSON compatible dictionary with recommended examples and fallback status.
    """
    import json

    if tenant_id is None:
        return json.dumps(
            {
                "error": "Tenant ID is required for recommend_examples.",
                "error_category": "invalid_request",
            }
        )
    import time

    start_time = time.monotonic()

    # Build kwargs to avoid passing None if we want to rely on service defaults,
    # though recommend_examples has a default of True.
    kwargs = {"question": query, "tenant_id": tenant_id, "limit": limit}
    if enable_fallback is not None:
        kwargs["enable_fallback"] = enable_fallback

    result = await RecommendationService.recommend_examples(**kwargs)

    execution_time_ms = (time.monotonic() - start_time) * 1000

    from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
    from dal.database import Database

    formatted_examples = result.model_dump()

    envelope = ToolResponseEnvelope(
        result=formatted_examples,
        metadata=GenericToolMetadata(
            provider=Database.get_query_target_provider(), execution_time_ms=execution_time_ms
        ),
    )
    return envelope.model_dump_json(exclude_none=True)

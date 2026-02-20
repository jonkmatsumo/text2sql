from typing import Optional

from mcp_server.services.recommendation.config import RECO_CONFIG
from mcp_server.services.recommendation.service import RecommendationService

TOOL_NAME = "recommend_examples"
TOOL_DESCRIPTION = "Recommend few-shot examples for a given natural language query."


async def handler(
    query: str,
    tenant_id: int,
    limit: int = RECO_CONFIG.limit_default,
    enable_fallback: Optional[bool] = None,
) -> str:
    """Recommend few-shot examples for a given natural language query.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher) and valid 'tenant_id'.

    Data Access:
        Read-only access to the few-shot Registry and interaction logs.
        Scoped by tenant_id.

    Failure Modes:
        - Unauthorized: If tenant_id is missing or role is insufficient.
        - Validation Error: If limit is out of bounds.
        - Dependency Failure: If the recommendation service is unavailable.

    Args:
        query: The user's natural language question.
        tenant_id: Tenant identifier (REQUIRED).
        limit: Maximum number of examples to recommend.
        enable_fallback: Whether to search for interactions if few-shots are insufficient.
            If None, uses service default (True).

    Returns:
        JSON string with recommended examples and fallback status.
    """
    from mcp_server.utils.auth import validate_role
    from mcp_server.utils.validation import require_tenant_id, validate_limit

    # 1. Validate inputs
    if err := validate_role("SQL_USER_ROLE", TOOL_NAME, tenant_id=tenant_id):
        return err
    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err
    if err := validate_limit(limit, TOOL_NAME):
        return err
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

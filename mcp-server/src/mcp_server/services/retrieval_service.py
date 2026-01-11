"""Retrieval module for dynamic few-shot learning."""

import json
import logging

from mcp_server.services.registry import RegistryService

logger = logging.getLogger(__name__)


async def get_relevant_examples(
    user_query: str,
    limit: int = 3,
    tenant_id: int = 1,
) -> str:
    """
    Retrieve few-shot examples similar to the user's query from the Registry.

    Args:
        user_query: The user's natural language question
        limit: Maximum number of examples to retrieve (default: 3)
        tenant_id: Tenant ID (default: 1)

    Returns:
        Formatted JSON string with examples, or empty string if none found
    """
    # 1. Fetch from Registry
    examples = await RegistryService.get_few_shot_examples(user_query, tenant_id, limit=limit)

    if not examples:
        return ""

    # 2. Format results
    results = [
        {"question": ex.question, "sql": ex.sql_query, "signature": ex.signature_key[:8]}
        for ex in examples
    ]

    return json.dumps(results, separators=(",", ":"))

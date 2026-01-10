"""Cache validation node using lightweight LLM."""

import logging

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate

logger = logging.getLogger(__name__)


async def cache_validate_node(state: AgentState) -> dict:
    """
    Validate if a cached SQL query matches the current user intent.

    Uses a lightweight LLM to compare the active query against the original
    cached query and SQL to prevent false positives (e.g., PG vs NC-17).

    Args:
        state: Current agent state

    Returns:
        dict: Updates to state (current_sql or rejected_cache_context)
    """
    with mlflow.start_span(
        name="cache_validate",
        span_type=mlflow.entities.SpanType.CHAIN,
    ) as span:
        active_query = state.get("active_query")
        if not active_query:
            messages = state["messages"]
            active_query = messages[-1].content if messages else ""

        cached_sql = state.get("cached_sql")
        cache_metadata = state.get("cache_metadata", {})
        original_query = cache_metadata.get("user_query", "Unknown")

        span.set_inputs(
            {
                "active_query": active_query,
                "cached_sql": cached_sql,
                "original_query": original_query,
            }
        )

        if not cached_sql:
            # Should not happen if routed correctly, but handle gracefully
            return {}

        # Use lightweight model for validation
        llm = get_llm_client(temperature=0, use_light_model=True)

        prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """You are a SQL Cache Validator.
Your task is to determine if the Cached SQL is valid for the New User Query.
Focus strictly on ENTITY matching (IDs, names, ratings, dates, specific values).
Ignore phrasing differences (e.g., "Show me" vs "List").

Return JSON: {{"valid": boolean, "reason": "concise explanation"}}
""",
                ),
                (
                    "user",
                    """New Query: {active_query}
Cached Query: {original_query}
Cached SQL: {cached_sql}
""",
                ),
            ]
        )

        chain = prompt | llm | JsonOutputParser()

        try:
            result = await chain.ainvoke(
                {
                    "active_query": active_query,
                    "original_query": original_query,
                    "cached_sql": cached_sql,
                }
            )

            is_valid = result.get("valid", False)
            reason = result.get("reason", "Unknown")

            span.set_outputs({"valid": is_valid, "reason": reason})

            if is_valid:
                logger.info(f"✓ Cache Validated. Reason: {reason}")
                return {
                    "current_sql": cached_sql,
                    "from_cache": True,
                    "rejected_cache_context": None,
                }
            else:
                logger.info(f"✗ Cache Rejected. Reason: {reason}")
                return {
                    "cached_sql": None,  # Clear invalid cache
                    "from_cache": False,
                    "rejected_cache_context": {
                        "sql": cached_sql,
                        "original_query": original_query,
                        "reason": reason,
                    },
                }

        except Exception as e:
            logger.error(f"Cache validation failed: {e}")
            # On error, play it safe and reject cache
            return {
                "cached_sql": None,
                "from_cache": False,
                "rejected_cache_context": None,  # Don't confuse generator with error
            }

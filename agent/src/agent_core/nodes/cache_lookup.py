"""Cache lookup and validation node."""

import logging

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools
from agent_core.utils.parsing import parse_tool_output

logger = logging.getLogger(__name__)


async def cache_lookup_node(state: AgentState) -> dict:
    """
    Node: CacheLookup (Entry Point).

    1. Checks semantic cache for similar queries.
    2. If hit, uses lightweight LLM to validate semantic equivalence.
    3. Returns cached SQL if valid, or rejected context if invalid.
    """
    with mlflow.start_span(
        name="cache_lookup",
        span_type=mlflow.entities.SpanType.RETRIEVER,
    ) as span:
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        span.set_inputs({"user_query": user_query})

        # 1. Lookup Cache
        tools = await get_mcp_tools()
        cache_tool = next((t for t in tools if t.name == "lookup_cache_tool"), None)

        if not cache_tool:
            logger.warning("lookup_cache_tool not found")
            return {"cached_sql": None}

        try:
            cache_json = await cache_tool.ainvoke({"user_query": user_query})
            cache_data = parse_tool_output(cache_json)

            if cache_data is None:
                logger.info("Cache lookup returned None (Cache Miss)")
                span.set_outputs({"hit": False})
                return {"cached_sql": None}

            if isinstance(cache_data, list) and len(cache_data) > 0:
                cache_data = cache_data[0]

            if not isinstance(cache_data, dict) or not cache_data.get("sql"):
                # Cache Miss
                logger.info(f"Cache miss or empty data. Data type: {type(cache_data)}")
                span.set_outputs({"hit": False})
                return {"cached_sql": None}

            # Cache Hit - Prepare for Validation
            cached_sql = cache_data.get("sql")
            metadata = cache_data.get("metadata", {})
            original_query = (
                metadata.get("user_query") or cache_data.get("original_query") or "Unknown"
            )

            span.set_attribute("potential_hit", True)
            span.set_attribute("original_query", original_query)

            # 2. Validate with Light LLM
            is_valid, validation_reason = await _validate_cache_hit(
                user_query, cached_sql, original_query
            )

            if is_valid:
                span.set_attribute("validation_status", "valid")
                span.set_outputs({"hit": True, "sql": cached_sql})
                return {
                    "current_sql": cached_sql,
                    "from_cache": True,
                    "cached_sql": cached_sql,  # For consistency
                }
            else:
                span.set_attribute("validation_status", "invalid")
                span.set_attribute("rejection_reason", validation_reason)

                # Create rejection context for GenerateNode
                rejected_context = {
                    "sql": cached_sql,
                    "original_query": original_query,
                    "reason": validation_reason,
                }
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "rejected_cache_context": rejected_context,
                }

        except Exception as e:
            logger.error(f"Cache lookup failed: {e}")
            return {"cached_sql": None}


async def _validate_cache_hit(
    new_query: str, cached_sql: str, original_query: str
) -> tuple[bool, str]:
    """Use lightweight LLM to validate cache hit."""
    try:
        # Use lightweight model for speed/cost
        client = get_llm_client(use_light_model=True)

        system_prompt = """You are a SQL Semantic Validator.
Compare the New Query with the Cached SQL (and its Original Query).
Determine if the Cached SQL is 100% valid for the New Query.

Rules:
1. Ignore trivial differences (case, whitespace, synonyms like "movies" vs "films").
2. Focus on ENTITIES (IDs, exact names, ratings).
3. "Top 10 actors in PG films" != "Top 10 actors in NC-17 films" -> INVALID.
4. "Sales in 2023" != "Sales in 2024" -> INVALID.

Return ONLY JSON:
{"valid": boolean, "reason": "concise explanation"}
"""

        user_prompt = f"""
New Query: {new_query}
Original Query: {original_query}
Cached SQL: {cached_sql}
"""
        messages = [
            ("system", system_prompt),
            ("user", user_prompt),
        ]

        response = await client.ainvoke(messages)
        content = response.content.strip()

        # Parse JSON output
        import json

        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("\n", 1)[0]

        result = json.loads(content)
        return result.get("valid", False), result.get("reason", "Unknown reason")

    except Exception as e:
        logger.error(f"Cache validation failed: {e}")
        # Fail safe - treat as invalid to trigger generation
        return False, f"Validation error: {e}"

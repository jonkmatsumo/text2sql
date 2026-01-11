"""Node for looking up queries in the semantic cache."""

import logging

import mlflow
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools
from agent_core.utils.parsing import parse_tool_output

logger = logging.getLogger(__name__)


async def cache_lookup_node(state: AgentState) -> dict:
    """
    Node: CacheLookup (Thin Client).

    1. Checks semantic cache via MCP tool.
    2. Tool handles extraction, exact-match, and deterministic validation.
    3. Returns cached SQL if valid, or miss if not.
    """
    with mlflow.start_span(
        name="cache_lookup",
        span_type=mlflow.entities.SpanType.RETRIEVER,
    ) as span:
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        span.set_inputs({"user_query": user_query})

        # 1. Lookup Cache via tool
        tools = await get_mcp_tools()
        cache_tool = next((t for t in tools if t.name == "lookup_cache"), None)

        if not cache_tool:
            logger.warning("lookup_cache tool not found")
            span.set_attribute("lookup_mode", "error")
            return {"cached_sql": None, "from_cache": False}

        try:
            tenant_id = state.get("tenant_id")
            cache_json = await cache_tool.ainvoke({"query": user_query, "tenant_id": tenant_id})
            cache_data = parse_tool_output(cache_json)

            if not cache_data:
                logger.info("Cache Miss or Rejected Hit")
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            if isinstance(cache_data, list) and len(cache_data) > 0:
                cache_data = cache_data[0]
            if not isinstance(cache_data, dict):
                logger.info("Cache Miss or Rejected Hit")
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            if not cache_data.get("value"):
                logger.info("Cache Miss or Rejected Hit")
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            # Cache Hit!
            cached_sql = cache_data.get("value")
            cache_id = cache_data.get("cache_id")
            similarity = cache_data.get("similarity", 1.0)  # Could be 1.0 for fingerprint

            logger.info(f"✓ Cache hit validated by MCP. ID: {cache_id}, Sim: {similarity:.4f}")
            span.set_outputs({"hit": True, "sql": cached_sql})
            return {
                "current_sql": cached_sql,
                "from_cache": True,
                "cached_sql": cached_sql,
            }

        except Exception as e:
            logger.error(f"Cache lookup failed: {e}")
            span.set_attribute("error", str(e))
            return {"cached_sql": None, "from_cache": False}

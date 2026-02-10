"""Node for looking up queries in the semantic cache."""

import logging
from typing import Optional

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.utils.parsing import parse_tool_output, unwrap_envelope
from common.config.env import get_env_bool

logger = logging.getLogger(__name__)


async def _get_current_schema_snapshot_id(
    tools, user_query: str, tenant_id: Optional[int]
) -> Optional[str]:
    from agent.utils.schema_cache import get_cached_schema_snapshot_id

    cached_snapshot_id = get_cached_schema_snapshot_id(tenant_id)
    if cached_snapshot_id:
        return cached_snapshot_id

    subgraph_tool = next((t for t in tools if t.name == "get_semantic_subgraph"), None)
    if not subgraph_tool:
        return None

    payload = {"query": user_query}
    if tenant_id is not None:
        payload["tenant_id"] = tenant_id
    try:
        raw_subgraph = await subgraph_tool.ainvoke(payload)
    except Exception as e:
        logger.warning(
            "Schema snapshot fetch failed",
            extra={"error_type": type(e).__name__, "error": str(e)},
            exc_info=True,
        )
        return None

    parsed = parse_tool_output(raw_subgraph)
    if isinstance(parsed, list) and parsed:
        parsed = parsed[0]

    # Unwrap GenericToolResponseEnvelope if present
    # Unwrap GenericToolResponseEnvelope if present
    parsed = unwrap_envelope(parsed)

    nodes = parsed.get("nodes", []) if isinstance(parsed, dict) else []
    from agent.utils.schema_cache import set_cached_schema_snapshot_id
    from agent.utils.schema_fingerprint import resolve_schema_snapshot_id

    snapshot_id = resolve_schema_snapshot_id(nodes)
    set_cached_schema_snapshot_id(tenant_id, snapshot_id)
    return snapshot_id


async def cache_lookup_node(state: AgentState) -> dict:
    """
    Node: CacheLookup (Thin Client).

    1. Checks semantic cache via MCP tool.
    2. Tool handles extraction, exact-match, and deterministic validation.
    3. Returns cached SQL if valid, or miss if not.
    """
    with telemetry.start_span(
        name="cache_lookup",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "cache_lookup")
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        span.set_inputs({"user_query": user_query})

        # 1. Lookup Cache via tool
        tools = await get_mcp_tools()
        cache_tool = next((t for t in tools if t.name == "lookup_cache"), None)

        if not cache_tool:
            logger.warning("lookup_cache tool not found")
            span.set_attribute("lookup_mode", "error")
            span.set_attribute("cache.hit", False)
            return {"cached_sql": None, "from_cache": False}

        try:
            tenant_id = state.get("tenant_id")
            cache_json = await cache_tool.ainvoke(
                {"query": user_query, "tenant_id": tenant_id}, config={}
            )
            cache_data = parse_tool_output(cache_json)

            if not cache_data:
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            if isinstance(cache_data, list) and len(cache_data) > 0:
                cache_data = cache_data[0]

            # Unwrap GenericToolResponseEnvelope if present
            # Unwrap GenericToolResponseEnvelope if present
            cache_data = unwrap_envelope(cache_data)

            if not isinstance(cache_data, dict):
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            if not cache_data.get("value"):
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            # Cache Hit!
            cached_sql = cache_data.get("value")
            cache_id = cache_data.get("cache_id")
            similarity = cache_data.get("similarity", 1.0)  # Could be 1.0 for fingerprint
            cache_metadata = cache_data.get("metadata") or {}

            span.set_attribute("cache.hit", True)
            span.set_attribute("cache.snapshot_missing", False)
            span.set_attribute("cache.snapshot_mismatch", False)
            if cache_id:
                span.set_attribute("cache.cache_id", cache_id)

            if get_env_bool("AGENT_CACHE_SCHEMA_VALIDATION", False):
                cached_snapshot_id = cache_metadata.get("schema_snapshot_id")
                if not cached_snapshot_id:
                    span.set_attribute("cache.snapshot_missing", True)
                else:
                    current_snapshot_id = await _get_current_schema_snapshot_id(
                        tools, user_query, tenant_id
                    )
                    span.set_attribute("cache.cached_snapshot_id", cached_snapshot_id)
                    if current_snapshot_id:
                        span.set_attribute("cache.current_snapshot_id", current_snapshot_id)
                        if cached_snapshot_id != current_snapshot_id:
                            span.set_attribute("cache.snapshot_mismatch", True)
                            logger.info(
                                "Rejecting cache hit due to schema mismatch",
                                extra={
                                    "cache_id": cache_id,
                                    "cached_snapshot_id": cached_snapshot_id,
                                    "current_snapshot_id": current_snapshot_id,
                                },
                            )
                            span.set_outputs({"hit": False, "reason": "schema_snapshot_mismatch"})
                            return {
                                "cached_sql": None,
                                "from_cache": False,
                                "cache_metadata": cache_metadata,
                                "cache_similarity": similarity,
                                "rejected_cache_context": {
                                    "sql": cached_sql,
                                    "original_query": user_query,
                                    "reason": "schema_snapshot_mismatch",
                                },
                            }

            logger.info(f"✓ Cache hit validated by MCP. ID: {cache_id}, Sim: {similarity:.4f}")
            span.set_outputs({"hit": True, "sql": cached_sql})
            return {
                "current_sql": cached_sql,
                "from_cache": True,
                "cached_sql": cached_sql,
                "cache_metadata": cache_metadata,
                "cache_similarity": similarity,
            }

        except Exception as e:
            logger.error(
                f"Cache lookup failed: {e}",
                extra={"error_type": type(e).__name__, "error": str(e)},
                exc_info=True,
            )
            span.set_attribute("error", str(e))
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("cache.hit", False)
            return {"cached_sql": None, "from_cache": False}


def reset_cache_state() -> None:
    """Reset the internal schema snapshot cache (test utility)."""
    from agent.utils.schema_cache import reset_schema_cache

    reset_schema_cache()

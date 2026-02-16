"""Node for looking up queries in the semantic cache."""

import logging
from typing import Optional

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.utils.parsing import parse_tool_output, unwrap_envelope
from agent.utils.schema_snapshot import resolve_pinned_schema_snapshot_id
from common.config.env import get_env_bool
from common.observability.metrics import agent_metrics

logger = logging.getLogger(__name__)


async def _get_current_schema_snapshot_id(
    tools, user_query: str, tenant_id: Optional[int]
) -> Optional[str]:
    from agent.utils.schema_cache import get_or_refresh_schema_snapshot_id

    subgraph_tool = next((t for t in tools if t.name == "get_semantic_subgraph"), None)
    if not subgraph_tool:
        return None

    async def _refresh_snapshot_id() -> Optional[str]:
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

        parsed = unwrap_envelope(parsed)
        nodes = parsed.get("nodes", []) if isinstance(parsed, dict) else []

        from agent.utils.schema_fingerprint import resolve_schema_snapshot_id

        return resolve_schema_snapshot_id(nodes)

    return await get_or_refresh_schema_snapshot_id(tenant_id, _refresh_snapshot_id)


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

        def _record_cache_outcome(outcome: str) -> None:
            agent_metrics.add_counter(
                "agent.cache.lookup_total",
                attributes={"outcome": outcome},
                description="Cache lookup outcomes (hit/miss/error)",
            )

        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "cache_lookup")
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        span.set_inputs({"user_query": user_query})

        # Bypass cache lookup if state already has a candidate (e.g. execute_sql mode)
        if state.get("from_cache") and state.get("current_sql"):
            logger.info("Bypassing cache lookup (SQL explicitly provided)")
            span.set_attribute("cache.bypass", True)
            span.set_outputs({"hit": True, "sql": state["current_sql"]})
            return {
                "cached_sql": state["current_sql"],
                "from_cache": True,
                "cache_lookup_failed": False,
            }

        # 1. Lookup Cache via tool
        tools = await get_mcp_tools()
        cache_tool = next((t for t in tools if t.name == "lookup_cache"), None)

        if not cache_tool:
            logger.warning("lookup_cache tool not found")
            span.set_attribute("lookup_mode", "error")
            span.set_attribute("cache.hit", False)
            span.set_attribute("cache.lookup_failed", True)
            span.set_attribute("cache.lookup_failure_reason", "tool_missing")
            _record_cache_outcome("error")
            return {
                "cached_sql": None,
                "from_cache": False,
                "cache_lookup_failed": True,
                "cache_lookup_failure_reason": "tool_missing",
            }

        try:
            tenant_id = state.get("tenant_id")
            cache_json = await cache_tool.ainvoke(
                {"query": user_query, "tenant_id": tenant_id}, config={}
            )
            cache_data = parse_tool_output(cache_json)

            if not cache_data:
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_attribute("cache.lookup_failed", False)
                span.set_outputs({"hit": False})
                _record_cache_outcome("miss")
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "cache_lookup_failed": False,
                    "cache_lookup_failure_reason": None,
                }

            if isinstance(cache_data, list) and len(cache_data) > 0:
                cache_data = cache_data[0]

            if isinstance(cache_data, dict) and cache_data.get("error"):
                error_obj = cache_data.get("error")
                error_category = (
                    error_obj.get("category", "tool_response_malformed")
                    if isinstance(error_obj, dict)
                    else "tool_response_malformed"
                )
                span.set_attribute("cache.lookup_failed", True)
                span.set_attribute("cache.lookup_failure_reason", "tool_error_payload")
                span.set_attribute("cache.lookup_error_category", str(error_category))
                span.add_event(
                    "cache.lookup_failure",
                    {"reason": "tool_error_payload", "category": str(error_category)},
                )
                _record_cache_outcome("error")
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "cache_lookup_failed": True,
                    "cache_lookup_failure_reason": "tool_error_payload",
                }

            # Unwrap GenericToolResponseEnvelope if present
            cache_data = unwrap_envelope(cache_data)

            if not isinstance(cache_data, dict):
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_attribute("cache.lookup_failed", True)
                span.set_attribute("cache.lookup_failure_reason", "malformed_cache_payload")
                span.add_event("cache.lookup_failure", {"reason": "malformed_cache_payload"})
                span.set_outputs({"hit": False})
                _record_cache_outcome("error")
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "cache_lookup_failed": True,
                    "cache_lookup_failure_reason": "malformed_cache_payload",
                }

            if not cache_data.get("value"):
                logger.info("Cache Miss or Rejected Hit")
                span.set_attribute("cache.hit", False)
                span.set_attribute("cache.lookup_failed", False)
                span.set_outputs({"hit": False})
                _record_cache_outcome("miss")
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "cache_lookup_failed": False,
                    "cache_lookup_failure_reason": None,
                }

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
                    current_snapshot_id = resolve_pinned_schema_snapshot_id(state)
                    if not current_snapshot_id or current_snapshot_id == "unknown":
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
                            _record_cache_outcome("miss")
                            return {
                                "cached_sql": None,
                                "from_cache": False,
                                "cache_metadata": cache_metadata,
                                "cache_similarity": similarity,
                                "cache_lookup_failed": False,
                                "cache_lookup_failure_reason": None,
                                "rejected_cache_context": {
                                    "sql": cached_sql,
                                    "original_query": user_query,
                                    "reason": "schema_snapshot_mismatch",
                                },
                            }
                    else:
                        span.set_attribute("cache.lookup_failed", True)
                        span.set_attribute(
                            "cache.lookup_failure_reason", "snapshot_validation_unavailable"
                        )
                        span.add_event(
                            "cache.lookup_failure",
                            {"reason": "snapshot_validation_unavailable"},
                        )
                        _record_cache_outcome("error")
                        return {
                            "cached_sql": None,
                            "from_cache": False,
                            "cache_lookup_failed": True,
                            "cache_lookup_failure_reason": "snapshot_validation_unavailable",
                        }

            logger.info(f"✓ Cache hit validated by MCP. ID: {cache_id}, Sim: {similarity:.4f}")
            span.set_outputs({"hit": True, "sql": cached_sql})
            _record_cache_outcome("hit")
            return {
                "current_sql": cached_sql,
                "from_cache": True,
                "cached_sql": cached_sql,
                "cache_metadata": cache_metadata,
                "cache_similarity": similarity,
                "cache_lookup_failed": False,
                "cache_lookup_failure_reason": None,
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
            span.set_attribute("cache.lookup_failed", True)
            span.set_attribute("cache.lookup_failure_reason", "exception")
            span.add_event(
                "cache.lookup_failure",
                {"reason": "exception", "error_type": type(e).__name__},
            )
            _record_cache_outcome("error")
            return {
                "cached_sql": None,
                "from_cache": False,
                "cache_lookup_failed": True,
                "cache_lookup_failure_reason": "exception",
            }


def reset_cache_state() -> None:
    """Reset the internal schema snapshot cache (test utility)."""
    from agent.utils.schema_cache import reset_schema_cache

    reset_schema_cache()

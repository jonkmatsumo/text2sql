import logging
import re

import mlflow
from agent_core.cache import extract_constraints, validate_sql_constraints
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools
from agent_core.utils.parsing import parse_tool_output

logger = logging.getLogger(__name__)

# Regex to detect potentially leaking tenant filters (e.g. store_id = 1)
# Adjust regex based on your known tenant columns
TENANT_LEAK_PATTERN = re.compile(r"(?i)\b(store_id|tenant_id)\s*=\s*['\"]?(\d+)['\"]?")


async def cache_lookup_node(state: AgentState) -> dict:
    """
    Node: CacheLookup (Entry Point).

    1. Checks semantic cache for similar queries.
    2. Extracts hard constraints from user query (rating, limit, etc.).
    3. Validates cached SQL against constraints using AST parsing (deterministic).
    4. Returns cached SQL if valid, or rejected context if invalid.
    """
    with mlflow.start_span(
        name="cache_lookup",
        span_type=mlflow.entities.SpanType.RETRIEVER,
    ) as span:
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        span.set_inputs({"user_query": user_query})

        # 1. Extract constraints from user query (deterministic, no LLM)
        constraints = extract_constraints(user_query)
        span.set_attribute("extracted_constraints", constraints.to_json())
        span.set_attribute("extraction_confidence", constraints.confidence)

        # 2. Lookup Cache
        tools = await get_mcp_tools()
        cache_tool = next((t for t in tools if t.name == "lookup_cache_tool"), None)

        if not cache_tool:
            logger.warning("lookup_cache_tool not found")
            span.set_attribute("lookup_mode", "error")
            return {"cached_sql": None, "from_cache": False}

        try:
            tenant_id = state.get("tenant_id")
            cache_json = await cache_tool.ainvoke(
                {"user_query": user_query, "tenant_id": tenant_id}
            )
            cache_data = parse_tool_output(cache_json)

            if cache_data is None:
                logger.info("Cache lookup returned None (Cache Miss)")
                span.set_attribute("lookup_mode", "miss")
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            if isinstance(cache_data, list) and len(cache_data) > 0:
                cache_data = cache_data[0]

            if not isinstance(cache_data, dict) or not cache_data.get("sql"):
                logger.info(f"Cache miss or empty data. Data type: {type(cache_data)}")
                span.set_attribute("lookup_mode", "miss")
                span.set_outputs({"hit": False})
                return {"cached_sql": None, "from_cache": False}

            # Cache Hit - Prepare for Validation
            cached_sql = cache_data.get("sql")
            cache_id = cache_data.get("cache_id")
            similarity = cache_data.get("similarity", 0.0)
            metadata = cache_data.get("metadata", {})
            original_query = (
                metadata.get("user_query") or cache_data.get("original_query") or "Unknown"
            )

            span.set_attribute("lookup_mode", "semantic_hit")
            span.set_attribute("potential_hit", True)
            span.set_attribute("cache_id", str(cache_id))
            span.set_attribute("similarity_score", similarity)
            span.set_attribute("original_query", original_query)

            # 3. Validate for Cross-Tenant Leaks (Safety Guardrail)
            # Ensure cached SQL doesn't contain hardcoded tenant IDs that differ from current
            if cached_sql and tenant_id:
                matches = TENANT_LEAK_PATTERN.findall(cached_sql)
                for col, val in matches:
                    if int(val) != int(tenant_id):
                        logger.warning(
                            f"Cache hit rejected due to tenant leak. Found {col}={val}, expected "
                            f"{tenant_id}. SQL: {cached_sql}"
                        )
                        span.set_attribute("lookup_mode", "leak_rejected")
                        span.set_attribute("rejection_reason", f"Tenant Leak: {col}={val}")
                        return {"cached_sql": None, "from_cache": False}

            # 4. Validate with Deterministic Constraint Guardrail (no LLM)
            validation = validate_sql_constraints(cached_sql, constraints)
            span.set_attribute("guardrail_verdict", "pass" if validation.is_valid else "fail")

            if validation.mismatches:
                mismatch_details = [m.to_dict() for m in validation.mismatches]
                span.set_attribute("mismatch_details", str(mismatch_details))

            if validation.is_valid:
                logger.info(f"Cache hit validated. ID: {cache_id}, Similarity: {similarity:.4f}")
                span.set_attribute("validation_status", "valid")
                span.set_outputs({"hit": True, "sql": cached_sql})
                return {
                    "current_sql": cached_sql,
                    "from_cache": True,
                    "cached_sql": cached_sql,
                }
            else:
                # Guardrail failed - treat as cache miss
                rejection_reasons = "; ".join(m.message for m in validation.mismatches)
                logger.warning(
                    f"Cache hit rejected by guardrail. ID: {cache_id}, "
                    f"Reason: {rejection_reasons}"
                )
                span.set_attribute("validation_status", "invalid")
                span.set_attribute("rejection_reason", rejection_reasons)

                # Create rejection context for GenerateNode (can use as template)
                rejected_context = {
                    "sql": cached_sql,
                    "original_query": original_query,
                    "reason": rejection_reasons,
                    "mismatches": [m.to_dict() for m in validation.mismatches],
                }

                span.set_outputs({"hit": False, "guardrail_rejected": True})
                return {
                    "cached_sql": None,
                    "from_cache": False,
                    "rejected_cache_context": rejected_context,
                }

        except Exception as e:
            logger.error(f"Cache lookup failed: {e}")
            span.set_attribute("lookup_mode", "error")
            span.set_attribute("error", str(e))
            return {"cached_sql": None, "from_cache": False}

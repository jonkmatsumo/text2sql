"""LangGraph workflow definition for Text 2 SQL agent with MLflow tracing."""

import inspect
import json
import logging
import re
import time
import uuid
from typing import Any, Optional

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from agent.nodes.cache_lookup import cache_lookup_node
from agent.nodes.clarify import clarify_node
from agent.nodes.correct import correct_sql_node
from agent.nodes.execute import validate_and_execute_node
from agent.nodes.generate import generate_sql_node
from agent.nodes.plan import plan_sql_node
from agent.nodes.retrieve import retrieve_context_node
from agent.nodes.router import router_node
from agent.nodes.synthesize import synthesize_insight_node
from agent.nodes.validate import validate_sql_node
from agent.nodes.visualize import visualize_query_node
from agent.state import AgentState
from agent.telemetry import SpanType, telemetry
from common.config.env import get_env_bool, get_env_float, get_env_int, get_env_str
from common.constants.reason_codes import RetryDecisionReason

logger = logging.getLogger(__name__)
_TRACE_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def run_telemetry_configure():
    """Configure telemetry at runtime to avoid import-time side effects."""
    # Configure Telemetry (OTEL only)
    telemetry.configure()


def with_telemetry_context(node_func):
    """Wrap a node function to restore telemetry context."""

    async def wrapped_node(state: AgentState):
        raw_ctx = state.get("telemetry_context")
        if raw_ctx:
            # Deserialize the context from the state
            ctx = telemetry.deserialize_context(raw_ctx)
            with telemetry.use_context(ctx):
                ret = node_func(state)
                if inspect.isawaitable(ret):
                    return await ret
                return ret
        ret = node_func(state)
        if inspect.isawaitable(ret):
            return await ret
        return ret

    wrapped_node.__name__ = node_func.__name__
    return wrapped_node


async def schema_refresh_node(state: AgentState) -> dict:
    """Node to handle automatic schema refresh on drift detection."""
    with telemetry.start_span(
        name="schema_refresh",
        span_type=SpanType.AGENT_NODE,
    ) as span:
        refresh_count = state.get("schema_refresh_count", 0)
        span.set_attribute("schema.drift.auto_refresh_attempted", True)
        span.set_attribute("schema.drift.refresh_count", refresh_count + 1)

        # Invalidate cache for missing identifiers
        from common.config.env import get_env_str
        from dal.schema_cache import SCHEMA_CACHE

        provider = get_env_str("QUERY_TARGET_BACKEND", "postgres")
        missing = state.get("missing_identifiers") or []
        span.set_attribute("schema.drift.missing_identifiers", missing)

        for identifier in missing:
            # Try to parse schema.table
            parts = identifier.split(".")
            if len(parts) == 2:
                SCHEMA_CACHE.invalidate(provider=provider, schema=parts[0], table=parts[1])
            else:
                # Fallback: invalidate table in default schema
                SCHEMA_CACHE.invalidate(provider=provider, table=identifier)

        return {
            "schema_refresh_count": refresh_count + 1,
            "error": None,  # Clear error to allow re-entry into the flow
            "schema_drift_suspected": False,
            "retry_count": state.get("retry_count", 0),  # Preserve retry count
        }


def route_after_router(state: AgentState) -> str:
    """
    Conditional edge logic after router node.

    Routes to clarify if ambiguity detected, otherwise to plan.
    Note: Retrieve has already run, so router has schema_context.

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    if state.get("ambiguity_type"):
        return "clarify"
    return "plan"


def route_after_cache_lookup(state: AgentState) -> str:
    """
    Conditional edge logic after cache lookup.

    Routes based on cache status:
    - If hit and valid (from_cache=True): go to AST validation (then execute)
    - If miss or invalid (from_cache=False): go to retrieve (schema lookup)
    """
    if state.get("from_cache"):
        return "validate"
    return "retrieve"


def route_after_validation(state: AgentState) -> str:
    """
    Conditional edge logic after SQL validation.

    Routes based on AST validation result:
    - If validation passed: go to execute
    - If validation failed: go to correction

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    ast_result = state.get("ast_validation_result")
    if ast_result and not ast_result.get("is_valid"):
        return "correct"
    # Also check for error set by validation
    if state.get("error"):
        return "correct"
    return "execute"


def _retry_policy_mode() -> str:
    mode = (get_env_str("AGENT_RETRY_POLICY", "static") or "static").strip().lower()
    if mode == "adaptive":
        return "adaptive"
    return "static"


def _max_retry_attempts() -> int:
    try:
        configured = get_env_int("AGENT_MAX_RETRIES", 3)
    except ValueError:
        logger.warning("Invalid AGENT_MAX_RETRIES value; defaulting to 3")
        configured = 3
    if configured is None:
        configured = 3
    return max(1, int(configured))


def _adaptive_is_retryable(error_category: str | None) -> bool:
    non_retryable = {
        "unsupported_capability",
        "auth",
        "invalid_request",
        "tool_response_malformed",
    }
    if not error_category:
        return True
    return error_category not in non_retryable


def route_after_execution(state: AgentState) -> str:
    """
    Conditional edge logic after SQL execution.

    Determines the next step based on execution result:
    - If error and retries < 3: go to correction
    - If error and retries >= 3: go to failure
    - If success: go to synthesis

    Args:
        state: Current agent state

    Returns:
        str: Next node name
    """
    if not state.get("error"):
        return "visualize"  # Go to visualization (then synthesis)

    error_category = state.get("error_category")
    if error_category == "unsupported_capability":
        span = telemetry.get_current_span()
        if span:
            span.set_attribute("retry.stopped_due_to_capability", True)
            span.add_event(
                "retry.decision",
                {
                    "category": "unsupported_capability",
                    "is_retryable": False,
                    "reason_code": RetryDecisionReason.UNSUPPORTED_CAPABILITY.value,
                    "will_retry": False,
                },
            )
        return "failed"

    # Guarded Automatic Schema Refresh
    if state.get("schema_drift_suspected") and state.get("schema_drift_auto_refresh"):
        refresh_count = state.get("schema_refresh_count", 0)
        if refresh_count < 1:
            return "refresh_schema"

    deadline_ts = state.get("deadline_ts")
    remaining = None
    estimated_correction_budget = _estimate_correction_budget_seconds(state)
    retry_count = state.get("retry_count", 0)
    max_retries = _max_retry_attempts()
    retry_policy = _retry_policy_mode()
    retry_after_seconds = state.get("retry_after_seconds")

    # Initialize or update retry_summary
    retry_summary = state.get("retry_summary") or {"attempts": [], "budget_exhausted": False}
    retry_summary["policy"] = retry_policy
    retry_summary["attempts"].append(
        {
            "retry_number": retry_count,
            "error_category": error_category,
            "timestamp": time.monotonic(),
        }
    )

    span = telemetry.get_current_span()
    if span:
        span.set_attribute("retry.policy", retry_policy)
        span.set_attribute("retry.max_retries", max_retries)
        span.set_attribute("retry.attempt_number", retry_count)

    if deadline_ts is not None:
        remaining = deadline_ts - time.monotonic()

    bounded_retry_after = 0.0
    required_budget = estimated_correction_budget
    retry_decision = {
        "category": error_category or "unknown",
        "policy": retry_policy,
        "retry_count": retry_count,
        "max_retries": max_retries,
        "remaining_budget": max(0.0, remaining) if remaining is not None else None,
    }

    if retry_policy == "adaptive":
        is_retryable = _adaptive_is_retryable(error_category)
        retry_summary["is_retryable"] = is_retryable
        retry_decision["is_retryable"] = is_retryable
        if span:
            span.set_attribute("retry.is_retryable", is_retryable)
        if not is_retryable:
            retry_summary["stopped_non_retryable"] = True
            state["retry_summary"] = retry_summary
            retry_decision["reason_code"] = RetryDecisionReason.NON_RETRYABLE_CATEGORY.value
            retry_decision["will_retry"] = False
            if span:
                span.add_event("retry.decision", retry_decision)
            return "failed"

        if retry_after_seconds is not None and float(retry_after_seconds) > 0:
            retry_decision["retry_after_raw"] = float(retry_after_seconds)
            if remaining is None:
                bounded_retry_after = float(retry_after_seconds)
                retry_decision["retry_after_applied"] = True
            else:
                bounded_retry_after = min(float(retry_after_seconds), max(0.0, remaining))
                retry_decision["retry_after_applied"] = bounded_retry_after > 0
                if bounded_retry_after < float(retry_after_seconds):
                    retry_decision["retry_after_capped"] = True

            if bounded_retry_after <= 0.0 and remaining is not None and remaining <= 0:
                retry_summary["budget_exhausted"] = True
                state["retry_summary"] = retry_summary
                state["error_category"] = "timeout"
                retry_decision["reason_code"] = (
                    RetryDecisionReason.BUDGET_EXHAUSTED_RETRY_AFTER.value
                )
                retry_decision["will_retry"] = False
                if span:
                    span.add_event("retry.decision", retry_decision)
                return "failed"
            state["retry_after_seconds"] = bounded_retry_after
            retry_summary["retry_after_seconds"] = bounded_retry_after
            required_budget += bounded_retry_after
            if span:
                span.set_attribute("retry.retry_after_seconds", bounded_retry_after)
        else:
            state["retry_after_seconds"] = None
    else:
        state["retry_after_seconds"] = None
        retry_decision["is_retryable"] = True  # Static policy treats all as retryable until max

    if deadline_ts is not None:
        retry_decision["required_budget"] = required_budget
        if span:
            span.set_attribute("retry.remaining_budget_seconds", max(0.0, remaining))
            span.set_attribute("retry.estimated_correction_budget", estimated_correction_budget)
            span.set_attribute("retry.budget.estimated_seconds", required_budget)
            span.set_attribute(
                "retry.budget.ema_latency_seconds", state.get("ema_llm_latency_seconds")
            )

        if remaining < required_budget:
            if span:
                span.set_attribute("retry.stopped_due_to_budget", True)
            retry_summary["budget_exhausted"] = True
            state["retry_summary"] = retry_summary
            state["error"] = (
                f"Retry budget exhausted after {retry_count} attempts; remaining time "
                f"{max(0.0, remaining):.2f}s is below estimated required "
                f"{required_budget:.2f}s."
            )
            state["error_category"] = "timeout"
            retry_decision["reason_code"] = RetryDecisionReason.INSUFFICIENT_BUDGET.value
            retry_decision["will_retry"] = False
            if span:
                span.add_event("retry.decision", retry_decision)
            return "failed"

    if span:
        span.set_attribute("retry.stopped_due_to_budget", False)

    if deadline_ts is not None and time.monotonic() >= deadline_ts:
        retry_summary["budget_exhausted"] = True
        state["retry_summary"] = retry_summary
        retry_decision["reason_code"] = RetryDecisionReason.DEADLINE_EXCEEDED.value
        retry_decision["will_retry"] = False
        if span:
            span.add_event("retry.decision", retry_decision)
        return "failed"

    if retry_count >= max_retries:
        retry_summary["max_retries_reached"] = True
        state["retry_summary"] = retry_summary
        retry_decision["reason_code"] = RetryDecisionReason.MAX_RETRIES_REACHED.value
        retry_decision["will_retry"] = False
        if span:
            span.add_event("retry.decision", retry_decision)
        return "failed"

    state["retry_summary"] = retry_summary
    retry_decision["reason_code"] = RetryDecisionReason.PROCEED_TO_CORRECTION.value
    retry_decision["will_retry"] = True
    if span:
        span.add_event("retry.decision", retry_decision)
    return "correct"  # Go to self-correction


def _estimate_correction_budget_seconds(state: AgentState) -> float:
    """Estimate time needed for another correction attempt."""
    min_budget = get_env_float("AGENT_MIN_RETRY_BUDGET_SECONDS", 3.0) or 0.0
    # Fixed overhead captures prompt assembly + orchestration costs.
    overhead_seconds = 0.5
    ema_latency = state.get("ema_llm_latency_seconds")
    observed = state.get("latency_correct_seconds") or state.get("latency_generate_seconds")
    if ema_latency is None and observed is not None:
        ema_latency = observed
    if ema_latency is None:
        ema_latency = min_budget or 3.0
    estimated = float(ema_latency) + overhead_seconds
    if min_budget:
        estimated = max(estimated, float(min_budget))
    return estimated


def create_workflow() -> StateGraph:
    """
    Create and configure the LangGraph workflow.

    Flow (schema-aware clarification):
    cache_lookup → [validate → execute]
    OR [retrieve → router → plan → generate → validate → execute]

    The key insight: explicit cache lookup node acts as entry point to optimize latency.

    Returns:
        StateGraph: Configured workflow graph (not compiled)
    """
    workflow = StateGraph(AgentState)

    # Add all nodes with telemetry context wrapping
    workflow.add_node("cache_lookup", with_telemetry_context(cache_lookup_node))
    workflow.add_node("router", with_telemetry_context(router_node))
    workflow.add_node("clarify", with_telemetry_context(clarify_node))
    workflow.add_node("retrieve", with_telemetry_context(retrieve_context_node))
    workflow.add_node("plan", with_telemetry_context(plan_sql_node))
    workflow.add_node("generate", with_telemetry_context(generate_sql_node))
    workflow.add_node("validate", with_telemetry_context(validate_sql_node))
    workflow.add_node("execute", with_telemetry_context(validate_and_execute_node))
    workflow.add_node("refresh_schema", with_telemetry_context(schema_refresh_node))
    workflow.add_node("correct", with_telemetry_context(correct_sql_node))
    workflow.add_node("visualize", with_telemetry_context(visualize_query_node))
    workflow.add_node("synthesize", with_telemetry_context(synthesize_insight_node))

    # Set entry point - Cache Lookup first
    workflow.set_entry_point("cache_lookup")

    # Routing from Cache Lookup
    workflow.add_conditional_edges(
        "cache_lookup",
        route_after_cache_lookup,
        {
            "validate": "validate",
            "retrieve": "retrieve",
        },
    )

    # Retrieve feeds into router (router now has schema context)
    workflow.add_edge("retrieve", "router")

    # Router conditional edges (schema-aware ambiguity detection)
    workflow.add_conditional_edges(
        "router",
        route_after_router,
        {
            "clarify": "clarify",
            "plan": "plan",
        },
    )

    # Clarify loops back to router (to re-evaluate with clarification)
    # Note: No need to re-retrieve since schema hasn't changed
    workflow.add_edge("clarify", "router")

    # Main flow edges
    workflow.add_edge("plan", "generate")
    workflow.add_edge("generate", "validate")

    # Validation conditional edges
    workflow.add_conditional_edges(
        "validate",
        route_after_validation,
        {
            "execute": "execute",
            "correct": "correct",
        },
    )

    # Execution conditional edges (self-correction loop)
    workflow.add_conditional_edges(
        "execute",
        route_after_execution,
        {
            "correct": "correct",
            "visualize": "visualize",
            "refresh_schema": "refresh_schema",
            "failed": END,
        },
    )

    # Refresh schema loops back to retrieve to get fresh DDLs
    workflow.add_edge("refresh_schema", "retrieve")

    # Visualization feeds into synthesis
    workflow.add_edge("visualize", "synthesize")

    # Correction loops back to validate (to re-check corrected SQL)
    workflow.add_edge("correct", "validate")

    # Final edge
    workflow.add_edge("synthesize", END)

    return workflow


# Create checkpointer for interrupt support
memory = MemorySaver()

# Compile workflow with checkpointer
app = create_workflow().compile(checkpointer=memory)


# Wrapper function with MLflow tracing
async def run_agent_with_tracing(
    question: str,
    tenant_id: int = 1,
    session_id: str = None,
    user_id: str = None,
    thread_id: str = None,
    schema_snapshot_id: str = None,
    timeout_seconds: float = None,
    deadline_ts: float = None,
    page_token: str = None,
    page_size: int = None,
    interactive_session: bool = False,
    replay_bundle: Optional[dict[str, Any]] = None,
) -> dict:
    """Run agent workflow with tracing and context propagation."""
    from langchain_core.messages import HumanMessage

    from common.sanitization import sanitize_text

    # 0. Centralized Ingress Sanitization
    raw_question = question
    res = sanitize_text(question)
    # We use the sanitized version for all downstream processing
    # If sanitization fails completely (e.g. empty after trim),
    # we use an empty string which will trigger failure/clarification naturally.
    question = res.sanitized or ""

    # Ensure telemetry is configured at runtime
    run_telemetry_configure()

    # Generate thread_id if not provided (required for checkpointer and telemetry)
    if thread_id is None:
        thread_id = session_id or str(uuid.uuid4())

    # Prepare base metadata for all spans
    base_metadata = {
        "tenant_id": str(tenant_id),
        "environment": get_env_str("ENVIRONMENT", "development"),
        "deployment": get_env_str("DEPLOYMENT", "development"),
        "version": "2.0.0",
        "thread_id": thread_id,
    }
    if replay_bundle:
        base_metadata["replay_mode"] = "active"

    if session_id:
        base_metadata["telemetry.session_id"] = session_id
    if user_id:
        base_metadata["telemetry.user_id"] = user_id

    with telemetry.start_span("agent_workflow", span_type=SpanType.CHAIN, attributes=base_metadata):
        # Make metadata sticky for all child spans
        telemetry.update_current_trace(base_metadata)

        # Capture context and serialize it for state persistence
        telemetry_context = telemetry.capture_context()
        serialized_ctx = telemetry.serialize_context(telemetry_context)

        # Prepare initial state
        if deadline_ts is None and timeout_seconds:
            deadline_ts = time.monotonic() + timeout_seconds

        inputs = {
            "messages": [HumanMessage(content=question)],
            "schema_context": "",
            "current_sql": None,
            "query_result": None,
            "error": None,
            "retry_after_seconds": None,
            "retry_count": 0,
            "schema_refresh_count": 0,
            # Reset state fields that shouldn't persist across turns
            "active_query": None,
            "procedural_plan": None,
            "rejected_cache_context": None,
            "clause_map": None,
            "tenant_id": tenant_id,
            "from_cache": False,
            "telemetry_context": serialized_ctx,
            "raw_user_input": raw_question,
            "schema_snapshot_id": schema_snapshot_id,
            "deadline_ts": deadline_ts,
            "timeout_seconds": timeout_seconds,
            "page_token": page_token,
            "page_size": page_size,
            "interactive_session": interactive_session,
            "replay_bundle": replay_bundle,
        }

        # Config with thread_id for checkpointer
        config = {"configurable": {"thread_id": thread_id}}

        # Execute workflow within MCP context to ensure connections are closed
        from agent.tools import mcp_tools_context, unpack_mcp_result
        from agent.utils.retry import retry_with_backoff

        # Interaction persistence mode (default: best_effort)
        persistence_mode = (get_env_str("AGENT_INTERACTION_PERSISTENCE_MODE", "") or "").strip()
        if not persistence_mode:
            legacy_fail_open = get_env_bool("PERSISTENCE_FAIL_OPEN", None)
            if legacy_fail_open is True:
                persistence_mode = "best_effort"
            elif legacy_fail_open is False:
                persistence_mode = "strict"
            else:
                persistence_mode = "best_effort"
        if persistence_mode not in {"best_effort", "strict"}:
            logger.warning(
                "Invalid AGENT_INTERACTION_PERSISTENCE_MODE: %s; defaulting to best_effort",
                persistence_mode,
            )
            persistence_mode = "best_effort"

        async with mcp_tools_context() as tools:
            schema_snapshot_id = inputs.get("schema_snapshot_id")
            if not schema_snapshot_id:
                snapshot_mode = get_env_str("SCHEMA_SNAPSHOT_MODE", "fingerprint").strip().lower()
                if snapshot_mode == "static":
                    schema_snapshot_id = "v1.0"
                elif snapshot_mode == "fingerprint":
                    subgraph_tool = next(
                        (t for t in tools if t.name == "get_semantic_subgraph"), None
                    )
                    if subgraph_tool:
                        try:
                            payload = {"query": question}
                            if tenant_id is not None:
                                payload["tenant_id"] = tenant_id
                            raw_subgraph = await subgraph_tool.ainvoke(payload)
                            from agent.utils.parsing import parse_tool_output
                            from agent.utils.schema_fingerprint import resolve_schema_snapshot_id

                            parsed = parse_tool_output(raw_subgraph)
                            if isinstance(parsed, list) and parsed:
                                parsed = parsed[0]
                            nodes = parsed.get("nodes", []) if isinstance(parsed, dict) else []
                            schema_snapshot_id = resolve_schema_snapshot_id(nodes)
                        except Exception:
                            logger.warning("Failed to compute schema snapshot id", exc_info=True)
                schema_snapshot_id = schema_snapshot_id or "unknown"
                inputs["schema_snapshot_id"] = schema_snapshot_id

            # 1. Start Interaction Logging (Pre-execution) with retry
            interaction_id = None
            interaction_persisted = False
            create_tool = next((t for t in tools if t.name == "create_interaction"), None)
            if create_tool:
                try:

                    async def _create_interaction():
                        # Use canonical OTEL trace_id if available, fallback to thread_id
                        # This ensures the ID stored in DB matches the trace in Tempo/Grafana
                        otel_trace_id = telemetry.get_current_trace_id()
                        final_trace_id = (
                            otel_trace_id
                            if otel_trace_id and _TRACE_ID_RE.fullmatch(otel_trace_id)
                            else None
                        )

                        return await create_tool.ainvoke(
                            {
                                "conversation_id": session_id or thread_id,
                                "schema_snapshot_id": schema_snapshot_id or "unknown",
                                "user_nlq_text": question,
                                "model_version": get_env_str("LLM_MODEL", "gpt-4o"),
                                "prompt_version": "v1.0",
                                "trace_id": final_trace_id,
                            },
                            config=config,
                        )

                    if telemetry.get_current_span():
                        telemetry.get_current_span().add_event("persistence.create.start")

                    raw_interaction_id = await retry_with_backoff(
                        _create_interaction,
                        "create_interaction",
                        extra_context={"trace_id": thread_id},
                    )
                    interaction_id = unpack_mcp_result(raw_interaction_id)
                    inputs["interaction_id"] = interaction_id
                    interaction_persisted = True
                    inputs["interaction_persisted"] = True
                    # Also make interaction_id sticky
                    telemetry.update_current_trace({"interaction_id": interaction_id})
                    if telemetry.get_current_span():
                        telemetry.get_current_span().add_event(
                            "persistence.create.success", {"interaction_id": interaction_id}
                        )
                except Exception as e:
                    # Structured logging with context (retry utility already logged attempts)
                    logger.error(
                        "Failed to create interaction after all retries",
                        extra={
                            "operation": "create_interaction",
                            "trace_id": thread_id,
                            "exception_type": type(e).__name__,
                            "exception_message": str(e),
                        },
                        exc_info=True,
                    )
                    if telemetry.get_current_span():
                        telemetry.get_current_span().add_event(
                            "persistence.create.failure",
                            {"exception": str(e), "type": type(e).__name__},
                        )
                    if telemetry.get_current_span():
                        telemetry.get_current_span().add_event(
                            "interaction.persist_failed",
                            {"stage": "create", "exception_type": type(e).__name__},
                        )
                    interaction_persisted = False
                    inputs["interaction_persisted"] = False
                    if persistence_mode == "strict":
                        raise RuntimeError(f"Interaction creation failed (mode=strict): {e}") from e
                    logger.warning(
                        "Continuing without interaction_id (best_effort)",
                        extra={"trace_id": thread_id},
                    )
            else:
                logger.warning("create_interaction tool not available")
                inputs["interaction_persisted"] = False

            # Execute workflow
            result = inputs.copy()
            try:
                result = await app.ainvoke(inputs, config=config)
                if "interaction_persisted" not in result:
                    result["interaction_persisted"] = interaction_persisted
            except Exception as execute_err:
                logger.error(
                    "Critical error in agent workflow",
                    extra={
                        "trace_id": thread_id,
                        "interaction_id": interaction_id,
                        "exception_type": type(execute_err).__name__,
                    },
                    exc_info=True,
                )
                result["error"] = str(execute_err)
                result["error_category"] = "SYSTEM_CRASH"
                if "messages" not in result:
                    result["messages"] = []

            # 2. Update Interaction Logging (Post-execution) with retry
            if interaction_id:
                update_tool = next((t for t in tools if t.name == "update_interaction"), None)
                if update_tool:
                    try:
                        # Determine status
                        status = "SUCCESS"
                        if result.get("error"):
                            status = "FAILURE"
                        elif result.get("ambiguity_type"):
                            status = "CLARIFICATION_REQUIRED"

                        # Get last message as response
                        last_msg = ""
                        if result.get("messages") and len(result["messages"]) > 0:
                            last_message_obj = result["messages"][-1]
                            if hasattr(last_message_obj, "content"):
                                last_msg = last_message_obj.content
                            else:
                                last_msg = str(last_message_obj)

                        if not last_msg and result.get("error"):
                            last_msg = f"System Error: {result['error']}"

                        # Capture update payload for retry closure
                        update_payload = {
                            "interaction_id": interaction_id,
                            "generated_sql": result.get("current_sql"),
                            "response_payload": json.dumps(
                                {"text": last_msg, "error": result.get("error")}
                            ),
                            "execution_status": status,
                            "error_type": result.get("error_category"),
                            "tables_used": result.get("table_names", []),
                        }

                        async def _update_interaction():
                            return await update_tool.ainvoke(update_payload, config=config)

                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event(
                                "persistence.update.start", {"interaction_id": interaction_id}
                            )

                        await retry_with_backoff(
                            _update_interaction,
                            "update_interaction",
                            extra_context={
                                "trace_id": thread_id,
                                "interaction_id": interaction_id,
                            },
                        )

                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event(
                                "persistence.update.success", {"interaction_id": interaction_id}
                            )
                    except Exception as e:
                        if telemetry.get_current_span():
                            telemetry.get_current_span().add_event(
                                "persistence.update.failure",
                                {"interaction_id": interaction_id, "exception": str(e)},
                            )
                            telemetry.get_current_span().add_event(
                                "interaction.persist_failed",
                                {"stage": "update", "exception_type": type(e).__name__},
                            )
                        # Structured logging - update failure is observable
                        logger.error(
                            "Failed to update interaction after all retries",
                            extra={
                                "operation": "update_interaction",
                                "trace_id": thread_id,
                                "interaction_id": interaction_id,
                                "exception_type": type(e).__name__,
                                "exception_message": str(e),
                            },
                            exc_info=True,
                        )
                        logger.error("Update failed for %s: %s", interaction_id, e)

                        # Mark result as having persistence failure (observable)
                        result["persistence_failed"] = True
                        result["persistence_error"] = str(e)
                        result["interaction_persisted"] = False
                        if persistence_mode == "strict":
                            raise RuntimeError(
                                f"Interaction update failed (mode=strict): {e}"
                            ) from e
                else:
                    logger.error("update_interaction tool not found in available tools")
                    logger.error("update_interaction tool missing!")
                    result["interaction_persisted"] = False

        # Metadata is already handled early and made sticky via telemetry_context

    return result

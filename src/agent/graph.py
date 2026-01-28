"""LangGraph workflow definition for Text 2 SQL agent with MLflow tracing."""

import inspect
import json
import logging
import re
import uuid

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
from common.config.env import get_env_bool, get_env_str

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
    if state.get("error"):
        if state.get("retry_count", 0) >= 3:
            return "failed"  # Go to graceful failure
        return "correct"  # Go to self-correction
    return "visualize"  # Go to visualization (then synthesis)


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
            "failed": END,
        },
    )

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
        inputs = {
            "messages": [HumanMessage(content=question)],
            "schema_context": "",
            "current_sql": None,
            "query_result": None,
            "error": None,
            "retry_count": 0,
            # Reset state fields that shouldn't persist across turns
            "active_query": None,
            "procedural_plan": None,
            "rejected_cache_context": None,
            "clause_map": None,
            "tenant_id": tenant_id,
            "from_cache": False,
            "telemetry_context": serialized_ctx,
            "raw_user_input": raw_question,
        }

        # Config with thread_id for checkpointer
        config = {"configurable": {"thread_id": thread_id}}

        # Execute workflow within MCP context to ensure connections are closed
        from agent.tools import mcp_tools_context, unpack_mcp_result
        from agent.utils.retry import retry_with_backoff

        # Check fail-open mode (default: fail-closed for reliability)
        persistence_fail_open = get_env_bool("PERSISTENCE_FAIL_OPEN", False)

        async with mcp_tools_context() as tools:
            # 1. Start Interaction Logging (Pre-execution) with retry
            interaction_id = None
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
                                "schema_snapshot_id": "v1.0",  # TODO: Dynamic snapshot ID
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
                    if not persistence_fail_open:
                        # Default: fail-closed - interaction persistence is required
                        raise RuntimeError(
                            f"Interaction creation failed (persistence_fail_open=False): {e}"
                        ) from e
                    # Fail-open mode: continue without interaction_id but emit warning
                    logger.warning(
                        "Continuing without interaction_id (PERSISTENCE_FAIL_OPEN=true)",
                        extra={"trace_id": thread_id},
                    )
            else:
                logger.warning("create_interaction tool not available")

            # Execute workflow
            result = inputs.copy()
            try:
                result = await app.ainvoke(inputs, config=config)
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
                        # Diagnostic print for immediate visibility
                        print(f"CRITICAL: Update failed for {interaction_id}: {e}")

                        # Mark result as having persistence failure (observable)
                        result["persistence_failed"] = True
                        result["persistence_error"] = str(e)
                else:
                    logger.error("update_interaction tool not found in available tools")
                    print("CRITICAL: update_interaction tool missing!")

        # Metadata is already handled early and made sticky via telemetry_context

    return result

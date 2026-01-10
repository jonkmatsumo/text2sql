"""LangGraph workflow definition for Text 2 SQL agent with MLflow tracing."""

import os

import mlflow
from agent_core.nodes.cache_validate import cache_validate_node
from agent_core.nodes.clarify import clarify_node
from agent_core.nodes.correct import correct_sql_node
from agent_core.nodes.execute import validate_and_execute_node
from agent_core.nodes.generate import generate_sql_node
from agent_core.nodes.plan import plan_sql_node
from agent_core.nodes.retrieve import retrieve_context_node
from agent_core.nodes.router import router_node
from agent_core.nodes.synthesize import synthesize_insight_node
from agent_core.nodes.validate import validate_sql_node
from agent_core.state import AgentState
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

# Configure MLflow tracking URI
# Default to localhost for local dev, but use container name in Docker
mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
mlflow.set_tracking_uri(mlflow_tracking_uri)

# Enable LangChain autologging with inline tracer for proper async context propagation
# This captures the entire graph execution as a hierarchical trace
mlflow.langchain.autolog(run_tracer_inline=True)


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
    if state.get("cached_sql"):
        return "cache_validate"
    return "plan"


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


def route_after_cache_validate(state: AgentState) -> str:
    """
    Conditional edge logic after cache validation.

    Routes based on validation result:
    - If valid (from_cache=True): go to execute
    - If invalid (from_cache=False): go to plan
    """
    if state.get("from_cache"):
        return "execute"
    return "plan"


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
    return "synthesize"  # Go to insight generation


def create_workflow() -> StateGraph:
    """
    Create and configure the LangGraph workflow.

    Flow (schema-aware clarification):
    retrieve → router → [clarify →] plan → generate → validate → execute → [correct →] synthesize

    The key insight: retrieve runs FIRST to populate schema_context,
    so the router can make schema-aware ambiguity decisions.

    Returns:
        StateGraph: Configured workflow graph (not compiled)
    """
    workflow = StateGraph(AgentState)

    # Add all nodes
    workflow.add_node("router", router_node)
    workflow.add_node("clarify", clarify_node)
    workflow.add_node("retrieve", retrieve_context_node)
    workflow.add_node("plan", plan_sql_node)
    workflow.add_node("generate", generate_sql_node)
    workflow.add_node("validate", validate_sql_node)
    workflow.add_node("execute", validate_and_execute_node)
    workflow.add_node("correct", correct_sql_node)
    workflow.add_node("cache_validate", cache_validate_node)
    workflow.add_node("synthesize", synthesize_insight_node)

    # Set entry point - retrieve runs first to populate schema_context
    workflow.set_entry_point("retrieve")

    # Retrieve feeds into router (router now has schema context)
    workflow.add_edge("retrieve", "router")

    # Router conditional edges (schema-aware ambiguity detection)
    workflow.add_conditional_edges(
        "router",
        route_after_router,
        {
            "clarify": "clarify",
            "cache_validate": "cache_validate",
            "plan": "plan",
        },
    )

    # Clarify loops back to router (to re-evaluate with clarification)
    # Note: No need to re-retrieve since schema hasn't changed
    workflow.add_edge("clarify", "router")

    # Cache Validation conditional edges
    workflow.add_conditional_edges(
        "cache_validate",
        route_after_cache_validate,
        {
            "execute": "execute",
            "plan": "plan",
        },
    )

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
            "synthesize": "synthesize",
            "failed": END,
        },
    )

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
    """
    Run agent workflow with MLflow tracing.

    The LangChain autologger automatically creates a root trace when the graph
    is invoked. This function enriches that trace with user/session metadata.

    Args:
        question: Natural language question
        tenant_id: Tenant identifier
        session_id: Session identifier for multi-turn conversations
        user_id: User identifier for attribution
        thread_id: Thread identifier for checkpointer state persistence
                   (required for interrupt/resume functionality)

    Returns:
        Agent state after workflow completion
    """
    import uuid

    from langchain_core.messages import HumanMessage

    # Prepare initial state
    inputs = {
        "messages": [HumanMessage(content=question)],
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": tenant_id,
    }

    # Generate thread_id if not provided (required for checkpointer)
    if thread_id is None:
        thread_id = session_id or str(uuid.uuid4())

    # Config with thread_id for checkpointer
    config = {"configurable": {"thread_id": thread_id}}

    # Execute workflow - autologger will create the root trace
    result = await app.ainvoke(inputs, config=config)

    # Enrich the trace with user/session metadata after invocation
    # Note: update_current_trace must be called within the trace context
    try:
        metadata = {
            "tenant_id": str(tenant_id),
            "environment": os.getenv("ENVIRONMENT", "development"),
            "deployment": os.getenv("DEPLOYMENT", "development"),
            "version": "2.0.0",  # Updated for new architecture
            "thread_id": thread_id,
        }
        if session_id:
            metadata["mlflow.trace.session"] = session_id
        if user_id:
            metadata["mlflow.trace.user"] = user_id

        mlflow.update_current_trace(metadata=metadata)
    except Exception:
        # Trace context may not be available outside the invoke
        pass

    return result

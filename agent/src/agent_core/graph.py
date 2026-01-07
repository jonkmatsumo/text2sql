"""LangGraph workflow definition for Text 2 SQL agent with MLflow tracing."""

import os

import mlflow
from agent_core.nodes.correct import correct_sql_node
from agent_core.nodes.execute import validate_and_execute_node
from agent_core.nodes.generate import generate_sql_node
from agent_core.nodes.retrieve import retrieve_context_node
from agent_core.nodes.synthesize import synthesize_insight_node
from agent_core.state import AgentState
from langgraph.graph import END, StateGraph

# Configure MLflow tracking URI
# Default to localhost for local dev, but use container name in Docker
mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
mlflow.set_tracking_uri(mlflow_tracking_uri)


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

    Returns:
        StateGraph: Compiled workflow graph
    """
    workflow = StateGraph(AgentState)

    # Add Nodes
    workflow.add_node("retrieve", retrieve_context_node)
    workflow.add_node("generate", generate_sql_node)
    workflow.add_node("execute", validate_and_execute_node)
    workflow.add_node("correct", correct_sql_node)
    workflow.add_node("synthesize", synthesize_insight_node)

    # Set Entry Point
    workflow.set_entry_point("retrieve")

    # Add Edges
    workflow.add_edge("retrieve", "generate")
    workflow.add_edge("generate", "execute")

    # Add Conditional Edge for Self-Correction Loop
    workflow.add_conditional_edges(
        "execute",
        route_after_execution,
        {
            "correct": "correct",
            "synthesize": "synthesize",
            "failed": END,
        },
    )

    workflow.add_edge("correct", "execute")  # Loop back to execution
    workflow.add_edge("synthesize", END)

    return workflow


# Compile workflow
app = create_workflow().compile()


# Wrapper function with MLflow tracing
async def run_agent_with_tracing(
    question: str,
    tenant_id: int = 1,
    session_id: str = None,
    user_id: str = None,
) -> dict:
    """
    Run agent workflow with MLflow tracing.

    Args:
        question: Natural language question
        tenant_id: Tenant identifier
        session_id: Session identifier for multi-turn conversations
        user_id: User identifier for attribution

    Returns:
        Agent state after workflow completion
    """
    # Start root span (MLflow 3.x uses start_span instead of start_trace)
    with mlflow.start_span(
        name="agent_workflow",
        span_type="AGENT",
    ) as trace:
        # Set trace inputs
        trace.set_inputs(
            {
                "question": question,
                "tenant_id": tenant_id,
            }
        )

        # Add contextual tags
        if session_id:
            trace.set_tag("session_id", session_id)
        if user_id:
            trace.set_tag("user_id", user_id)
        trace.set_tag("tenant_id", str(tenant_id))
        trace.set_tag("environment", os.getenv("ENVIRONMENT", "development"))
        trace.set_tag("deployment", os.getenv("DEPLOYMENT", "development"))
        trace.set_tag("version", "1.0.0")

        # Prepare initial state
        from langchain_core.messages import HumanMessage

        inputs = {
            "messages": [HumanMessage(content=question)],
            "schema_context": "",
            "current_sql": None,
            "query_result": None,
            "error": None,
            "retry_count": 0,
            "tenant_id": tenant_id,
        }

        # Execute workflow
        result = await app.ainvoke(inputs)

        # Set trace outputs
        trace.set_outputs(
            {
                "sql": result.get("current_sql"),
                "has_result": result.get("query_result") is not None,
                "error": result.get("error"),
                "retry_count": result.get("retry_count", 0),
            }
        )

        return result

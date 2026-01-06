"""LangGraph workflow definition for Text 2 SQL agent."""

from agent_core.nodes.correct import correct_sql_node
from agent_core.nodes.execute import validate_and_execute_node
from agent_core.nodes.generate import generate_sql_node
from agent_core.nodes.retrieve import retrieve_context_node
from agent_core.nodes.synthesize import synthesize_insight_node
from agent_core.state import AgentState
from langgraph.graph import END, StateGraph


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

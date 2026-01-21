"""Clarification node using LangGraph interrupt for human-in-the-loop.

This module implements the clarification flow that:
1. Pauses execution when ambiguity is detected
2. Surfaces clarification question to the user
3. Resumes with user's response
"""

from agent_core.state import AgentState
from agent_core.telemetry import telemetry
from agent_core.telemetry_schema import SpanKind, TelemetryKeys

# Try to import interrupt from langgraph.types (LangGraph 0.2+)
try:
    from langgraph.types import interrupt
except ImportError:
    # Fallback for older LangGraph versions
    interrupt = None


async def clarify_node(state: AgentState) -> dict:
    """
    Node: Clarify.

    Pauses execution for user clarification using LangGraph interrupt().
    This enables human-in-the-loop interaction for ambiguous queries.

    When executed:
    1. Reads clarification_question from state
    2. Calls interrupt() to pause graph and surface question to user
    3. On resume, receives user's response
    4. Returns updated state with user_clarification

    Note: Requires checkpointer (e.g., MemorySaver) for state persistence.

    Args:
        state: Current agent state with clarification_question

    Returns:
        dict: Updated state with user_clarification
    """
    with telemetry.start_span(
        name="clarify",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "clarify")
        clarification_question = state.get("clarification_question")
        ambiguity_type = state.get("ambiguity_type")

        span.set_inputs(
            {
                "clarification_question": clarification_question,
                "ambiguity_type": ambiguity_type,
            }
        )

        if not clarification_question:
            span.set_outputs({"error": "No clarification question"})
            return {}

        # Use LangGraph interrupt if available
        if interrupt is not None:
            # Interrupt execution and wait for user response
            # The interrupt payload is surfaced to the client
            user_response = interrupt(
                {
                    "type": "clarification_needed",
                    "question": clarification_question,
                    "ambiguity_type": ambiguity_type,
                    "resolved_bindings": state.get("resolved_bindings", {}),
                }
            )

            span.set_outputs(
                {
                    "user_response_received": True,
                    "response_length": len(str(user_response)) if user_response else 0,
                    # Capture actual response for debugging (truncated for safety)
                    "user_response": str(user_response)[:1000] if user_response else None,
                }
            )

            # Append interaction to conversation history so context is preserved
            from langchain_core.messages import AIMessage, HumanMessage

            new_messages = []
            if clarification_question:
                new_messages.append(AIMessage(content=clarification_question))

            if user_response:
                new_messages.append(HumanMessage(content=str(user_response)))

            return {
                "user_clarification": user_response,
                "ambiguity_type": None,  # Clear after getting response
                "clarification_question": None,
                "messages": new_messages,
            }
        else:
            # Fallback: Log warning and proceed without clarification
            # This allows the system to work without checkpointer
            span.set_attribute("interrupt_unavailable", "true")
            span.set_outputs(
                {
                    "warning": "LangGraph interrupt not available",
                    "fallback": "proceeding_without_clarification",
                }
            )

            return {
                "user_clarification": None,
                "ambiguity_type": None,
                "clarification_question": None,
            }

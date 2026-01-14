"""Insight synthesis node for formatting results with MLflow tracing."""

import json

from agent_core.llm_client import get_llm
from agent_core.state import AgentState
from agent_core.telemetry import SpanType, telemetry
from dotenv import load_dotenv
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize LLM using the factory (temperature=0.7 for more creative responses)


def synthesize_insight_node(state: AgentState) -> dict:
    """
    Node 6: SynthesizeInsight.

    Formats the query result into a natural language response.

    Args:
        state: Current agent state with query_result

    Returns:
        dict: Updated state with synthesized response in messages
    """
    with telemetry.start_span(
        name="synthesize_insight",
        span_type=SpanType.CHAT_MODEL,
    ) as span:
        query_result = state["query_result"]

        # Get the original question from the LAST user message (not first)
        # This is important because checkpointer accumulates messages across turns
        original_question = ""
        if state["messages"]:
            from langchain_core.messages import HumanMessage

            # Find the last HumanMessage (the current question)
            for msg in reversed(state["messages"]):
                if isinstance(msg, HumanMessage):
                    original_question = msg.content
                    break

        span.set_inputs(
            {
                "question": original_question,
                "result_count": len(query_result) if query_result else 0,
            }
        )

        if not query_result:
            response_content = "I couldn't retrieve any results for your query."
            span.set_outputs({"response": response_content})
            return {
                "messages": [
                    AIMessage(content=response_content),
                ]
            }

        # Format result as JSON string for LLM
        result_str = json.dumps(query_result, indent=2, default=str)

        system_prompt = """You are a helpful data analyst assistant.
Format the query results into a clear, natural language response.
Be concise but informative. Use numbers and data from the results.
"""

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                (
                    "user",
                    "Question: {question}\n\nQuery Results:\n{results}\n\nProvide a clear answer:",
                ),
            ]
        )

        chain = prompt | get_llm(temperature=0.7)

        response = chain.invoke(
            {
                "question": original_question,
                "results": result_str,
            }
        )

        # Capture token usage
        from agent_core.llm_client import extract_token_usage

        usage_stats = extract_token_usage(response)
        if usage_stats:
            span.set_attributes(usage_stats)

        span.set_outputs({"response_length": len(response.content)})

        return {
            "messages": [AIMessage(content=response.content)],
        }

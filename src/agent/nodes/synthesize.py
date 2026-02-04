"""Insight synthesis node for formatting results with MLflow tracing."""

import json

from dotenv import load_dotenv
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

from agent.llm_client import get_llm
from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys

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
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "synthesize_insight")
        query_result = state["query_result"]
        result_is_truncated = state.get("result_is_truncated")
        result_row_limit = state.get("result_row_limit")
        result_rows_returned = state.get("result_rows_returned")
        result_columns = state.get("result_columns")

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
        span.set_attribute("result.is_truncated", bool(result_is_truncated))
        if result_row_limit is not None:
            span.set_attribute("result.row_limit", result_row_limit)
        if result_rows_returned is not None:
            span.set_attribute("result.rows_returned", result_rows_returned)

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

        column_hints = ""
        if isinstance(result_columns, list) and result_columns:
            hint_lines = []
            max_columns = 50
            max_type_len = 48
            for col in result_columns[:max_columns]:
                if not isinstance(col, dict):
                    continue
                name = str(col.get("name", "")).strip()
                type_hint = col.get("type") or col.get("db_type") or "unknown"
                type_hint = str(type_hint)
                if len(type_hint) > max_type_len:
                    type_hint = f"{type_hint[: max_type_len - 3]}..."
                if name:
                    hint_lines.append(f"- {name}: {type_hint}")
            if hint_lines:
                column_hints = "Column types:\n" + "\n".join(hint_lines)

        system_prompt = """You are a helpful data analyst assistant.
Format the query results into a clear, natural language response.
Be concise but informative. Use numbers and data from the results.
"""

        user_prompt = (
            "Question: {question}\n\n"
            "Query Results:\n{results}\n\n"
            "{column_hints}\n\n"
            "Provide a clear answer:"
        )

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("user", user_prompt),
            ]
        )

        chain = prompt | get_llm(temperature=0.7)

        response = chain.invoke(
            {
                "question": original_question,
                "results": result_str,
                "column_hints": column_hints,
            }
        )

        # Capture token usage
        from agent.llm_client import extract_token_usage

        usage_stats = extract_token_usage(response)
        if usage_stats:
            span.set_attributes(usage_stats)

        response_content = response.content
        if result_is_truncated:
            warning = "Note: Results are truncated"
            if result_row_limit:
                warning += f" to {result_row_limit} rows"
            if result_rows_returned is not None:
                warning += f" (showing {result_rows_returned})"
            warning += "."
            response_content = f"{warning}\n\n{response_content}"

        span.set_outputs({"response_length": len(response_content)})

        return {
            "messages": [AIMessage(content=response_content)],
        }

"""Insight synthesis node for formatting results with MLflow tracing."""

import json
import re

from dotenv import load_dotenv
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

from agent.config import get_synthesize_temperature
from agent.llm_client import get_llm
from agent.models.termination import TerminationReason
from agent.state import AgentState
from agent.state.result_completeness import PartialReason, ResultCompleteness
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from common.config.env import get_env_bool

load_dotenv()

# Initialize LLM using the factory (temperature=0.7 for more creative responses)


def _sanity_check_enabled() -> bool:
    return get_env_bool("AGENT_EMPTY_RESULT_SANITY_CHECK", True) is True


def _question_implies_existence(question: str) -> bool:
    """Check if the question implies that data should exist.

    Returns True for queries that use superlatives, rankings, or specific entity
    references that would typically be expected to return results.
    """
    if not question:
        return False
    q = question.lower()

    # Superlatives and ranking terms
    superlative_patterns = (
        r"\b(top|latest|most|highest|lowest|newest|recent|last|"
        r"biggest|best|worst|oldest|first|maximum|minimum)\b"
    )
    if re.search(superlative_patterns, q):
        return True

    # Aggregation terms that imply data exists
    if re.search(r"\b(average|total|sum|count|how many|how much)\b", q):
        return True

    # Specific entity references (names, IDs, etc.)
    entity_terms = r"\b(named|called|id number|customer|user|order|product)\b"
    if re.search(entity_terms, q):
        return True

    return False


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
        result_is_limited = state.get("result_is_limited")
        result_limit = state.get("result_limit")
        completeness_payload = state.get("result_completeness")
        if isinstance(completeness_payload, dict):
            completeness = ResultCompleteness(
                rows_returned=int(completeness_payload.get("rows_returned", 0)),
                is_truncated=bool(completeness_payload.get("is_truncated", False)),
                is_limited=bool(completeness_payload.get("is_limited", False)),
                row_limit=completeness_payload.get("row_limit"),
                query_limit=completeness_payload.get("query_limit"),
                next_page_token=completeness_payload.get("next_page_token"),
                page_size=completeness_payload.get("page_size"),
                partial_reason=completeness_payload.get("partial_reason"),
            )
        else:
            completeness = ResultCompleteness.from_parts(
                rows_returned=int(
                    result_rows_returned
                    if result_rows_returned is not None
                    else (len(query_result) if query_result else 0)
                ),
                is_truncated=bool(result_is_truncated),
                is_limited=bool(result_is_limited),
                row_limit=result_row_limit,
                query_limit=result_limit if result_is_limited else None,
                next_page_token=None,
            )

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
        span.set_attribute("result.is_truncated", bool(completeness.is_truncated))
        if completeness.row_limit is not None:
            span.set_attribute("result.row_limit", completeness.row_limit)
        span.set_attribute("result.rows_returned", completeness.rows_returned)
        span.set_attribute("result.is_limited", bool(completeness.is_limited))
        if completeness.query_limit is not None:
            span.set_attribute("result.limit", completeness.query_limit)

        termination_reason = state.get("termination_reason")
        if termination_reason:
            span.set_attribute("termination_reason", termination_reason)
        error = state.get("error")

        if error:
            # 1. Redact potentially sensitive info first
            from common.sanitization.text import redact_sensitive_info

            error_str = redact_sensitive_info(str(error))

            error_category = state.get("error_category")

            # 2. Map termination reason to user-facing messages
            if termination_reason == TerminationReason.READONLY_VIOLATION:
                response_content = (
                    "I cannot perform this operation. The system is currently in read-only mode "
                    "to ensure data integrity."
                )
            elif termination_reason == TerminationReason.PERMISSION_DENIED:
                response_content = (
                    "I am unable to access the requested data due to insufficient permissions. "
                    "Please contact your administrator if you believe this is an error."
                )
            elif termination_reason == TerminationReason.BUDGET_EXHAUSTED:
                response_content = (
                    "The processing for this request exceeded the allowed resource budget. "
                    "Please try a simpler or more specific query."
                )
            elif termination_reason == TerminationReason.TIMEOUT:
                response_content = (
                    "The request timed out while waiting for the database to respond. "
                    "The query might be too complex or the database is under heavy load."
                )
            elif termination_reason == TerminationReason.SCHEMA_CHANGED:
                response_content = (
                    "The database schema appears to have changed during your request. "
                    "Please try again in a few moments."
                )
            elif (
                termination_reason == TerminationReason.VALIDATION_FAILED
                or termination_reason == TerminationReason.INVALID_REQUEST
                or termination_reason == TerminationReason.TIMEOUT
                or error_category in ("invalid_request", "timeout")
            ):
                response_content = (
                    f"I encountered a validation error while processing your request: {error_str}"
                )
            elif (
                termination_reason == TerminationReason.UNSUPPORTED_CAPABILITY
                or error_category == "unsupported_capability"
            ):
                error_metadata = state.get("error_metadata") or {}
                cap = error_metadata.get("required_capability")
                if cap:
                    response_content = (
                        f"The database backend does not support the required capability '{cap}' "
                        "for this request."
                    )
                else:
                    response_content = (
                        "The database backend does not support a required capability "
                        "for this request."
                    )
            elif (
                termination_reason == TerminationReason.TOOL_RESPONSE_MALFORMED
                or error_category == "tool_response_malformed"
            ):
                response_content = f"I received a malformed response from the database: {error_str}"
            else:
                # Generic fallback for unknown/other errors
                response_content = "An internal error occurred while processing your request."

            span.set_outputs({"error_response": response_content})
            return {
                "messages": [
                    AIMessage(content=response_content),
                ]
            }

        if query_result is None:
            response_content = "I couldn't retrieve any results for your query."
            span.set_outputs({"response": response_content})
            return {
                "messages": [
                    AIMessage(content=response_content),
                ]
            }

        if isinstance(query_result, list) and len(query_result) == 0:
            response_lines = [
                "I couldn't find any rows matching your query.",
                "Try widening your filters or adjusting the time range.",
            ]
            guidance_lines = []
            if state.get("schema_drift_suspected"):
                msg = (
                    "The schema may have changed; consider refreshing schema context and retrying."
                )
                response_lines.append(msg)
                guidance_lines.append(msg)

            if _sanity_check_enabled() and _question_implies_existence(original_question):
                span.set_attribute("result.sanity_check_triggered", True)
                msg = "If you expected results, double-check filters or try a broader query."
                response_lines.append(msg)
                guidance_lines.append(msg)

            response_content = " ".join(response_lines)
            span.set_outputs({"response": response_content})
            return {
                "messages": [
                    AIMessage(content=response_content),
                ],
                "empty_result_guidance": " ".join(guidance_lines) if guidance_lines else None,
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

        chain = prompt | get_llm(temperature=get_synthesize_temperature(), seed=state.get("seed"))

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
        warnings = []
        if completeness.is_truncated:
            if completeness.partial_reason == PartialReason.PROVIDER_CAP.value:
                if completeness.row_limit is not None:
                    warnings.append(
                        f"Note: The backend capped results at {completeness.row_limit} rows."
                    )
                else:
                    warnings.append("Note: The backend capped results.")
            else:
                warning = "Note: Results are truncated"
                if completeness.row_limit:
                    warning += f" to {completeness.row_limit} rows"
                if completeness.rows_returned is not None:
                    warning += f" (showing {completeness.rows_returned})"
                warning += "."
                warnings.append(warning)
        if completeness.is_limited:
            if completeness.query_limit is not None:
                warnings.append(
                    "Note: This query is limited to the top "
                    f"{completeness.query_limit} rows (ORDER BY/LIMIT)."
                )
            else:
                warnings.append("Note: This query is limited (ORDER BY/LIMIT).")
        if completeness.next_page_token:
            warnings.append("Note: More results are available; request the next page to continue.")
        if warnings:
            warning_block = "\n".join(warnings)
            response_content = f"{warning_block}\n\n{response_content}"

        span.set_outputs({"response_length": len(response_content)})

        return {
            "messages": [AIMessage(content=response_content)],
        }

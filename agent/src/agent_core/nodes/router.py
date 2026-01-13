"""Router node for intent classification and ambiguity detection.

This module implements the entry point that:
1. Analyzes the user's natural language query
2. Detects potential ambiguities (schema reference, value, temporal, metric)
3. Routes to clarification or retrieval based on ambiguity detection
"""

import json

from agent_core.state import AgentState
from agent_core.telemetry import SpanType, telemetry
from agent_core.tools import get_mcp_tools
from agent_core.utils.parsing import parse_tool_output
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

load_dotenv()


CONTEXTUALIZE_SYSTEM_PROMPT = """You are a helpful assistant that reformulates follow-up questions.
Given the conversation history and the latest user question, rephrase the latest question into a
standalone question that can be understood without the chat history.
DO NOT answer the question. JUST rephrase it.
If the latest question is already standalone, return it as is.
"""

CLARIFICATION_SYSTEM_PROMPT = """You are a helpful data assistant.
An ambiguity or missing data issue was detected in a user's SQL query.

Your task:
1. If the status is AMBIGUOUS: Generate a polite clarification question
   based ONLY on the provided options.
   - Use NATURAL LANGUAGE. Do NOT use internal schema names (e.g., `table.column`, snake_case).
   - BAD: "Do you mean language.name = 'Spanish'?"
   - GOOD:
     "Do you mean films spoken in Spanish, or films with Spanish as the original language?"

2. If the status is MISSING: Generate a helpful refusal message
   explaining what's missing and suggest alternatives based on the Schema Context.
   - Be specific about what concept is missing but use natural terms
     (e.g., "rating" instead of "film.rating").

## Ambiguity Data
{ambiguity_data}

## Schema Context
{schema_context}

Output ONLY the question or refusal message for the user.
"""


async def router_node(state: AgentState) -> dict:
    """
    Node: Router.

    Entry point that classifies intent and detects ambiguity.
    Routes to clarification node if ambiguous, otherwise to retrieval.

    Args:
        state: Current agent state with messages

    Returns:
        dict: Updated state with ambiguity_type and clarification_question (if needed)
    """
    with telemetry.start_span(
        name="router",
        span_type=SpanType.CHAIN,
    ) as span:
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        schema_context = state.get("schema_context", "")

        # 1. Contextualize Query (if history exists)
        # ---------------------------------------------------------------------
        active_query = user_query

        # Check if we have history (more than just the current user message)
        # Note: messages typically alternates Human, AI, Human...
        if len(messages) > 1:
            try:
                # Use history to reformulate
                contextualize_prompt = ChatPromptTemplate.from_messages(
                    [
                        ("system", CONTEXTUALIZE_SYSTEM_PROMPT),
                        MessagesPlaceholder(variable_name="chat_history"),
                        ("human", "{question}"),
                    ]
                )
                from agent_core.llm_client import get_llm

                contextualize_chain = contextualize_prompt | get_llm(temperature=0)

                # Exclude the last message (current user query) from history
                history_messages = messages[:-1]

                reformulated = await contextualize_chain.ainvoke(
                    {"chat_history": history_messages, "question": user_query}
                )

                active_query = reformulated.content
                span.set_attribute("contextualized_query", active_query)
                print(f"Contextualized Query: {active_query}")

            except Exception as e:
                print(f"Warning: Contextualization failed: {e}")

        # Store active query in span for observability
        span.set_inputs({"user_query": user_query, "active_query": active_query})

        # 2. Check for existing clarification (Interrupt Resume)
        # ---------------------------------------------------------------------
        user_clarification = state.get("user_clarification")
        if user_clarification:
            # Clear ambiguity and proceed
            # Note: We keep active_query as is, or maybe we should have appended clarification?
            # But Contextualization step likely handled it if history was present.
            span.set_outputs({"action": "proceed_with_clarification"})
            span.set_outputs({"action": "proceed_with_clarification"})
            return {
                "ambiguity_type": None,
                "clarification_question": None,
                "active_query": active_query,
                "user_clarification": None,  # Consumed and cleared
            }

        if not active_query:
            span.set_outputs({"error": "No query to route"})
            return {}

        # 3. Deterministic Ambiguity Detection
        # ---------------------------------------------------------------------
        tools = await get_mcp_tools()
        resolver_tool = next((t for t in tools if t.name == "resolve_ambiguity"), None)

        raw_schema = state.get("raw_schema_context", [])
        res_data = {"status": "CLEAR"}  # Fallback

        if resolver_tool:
            res_json = await resolver_tool.ainvoke(
                {"query": active_query, "schema_context": raw_schema}
            )
            parsed_res = parse_tool_output(res_json)
            if isinstance(parsed_res, list) and len(parsed_res) > 0:
                res_data = parsed_res[0]
        else:
            print("Warning: resolve_ambiguity tool not found.")

        status = res_data.get("status", "CLEAR")
        span.set_attribute("resolution_status", status)

        if status in ("AMBIGUOUS", "MISSING"):
            # Use LLM to phrase the question/refusal nicely
            prompt = ChatPromptTemplate.from_messages([("system", CLARIFICATION_SYSTEM_PROMPT)])
            from agent_core.llm_client import get_llm

            chain = prompt | get_llm(temperature=0)
            response = await chain.ainvoke(
                {
                    "ambiguity_data": json.dumps(res_data),
                    "schema_context": schema_context,
                }
            )

            clarification_msg = response.content.strip()
            ambiguity_type = res_data.get("ambiguity_type")
            if not ambiguity_type:
                ambiguity_type = "AMBIGUOUS" if status == "AMBIGUOUS" else "MISSING_DATA"

            span.set_outputs(
                {
                    "action": "clarify" if status == "AMBIGUOUS" else "refuse",
                    "ambiguity_type": ambiguity_type,
                }
            )

            return {
                "ambiguity_type": ambiguity_type,
                "clarification_question": clarification_msg,
                "active_query": active_query,
                "resolved_bindings": res_data.get("resolved_bindings", {}),
            }

        # CLEAR QUERY: Proceed to plan
        span.set_outputs({"action": "plan"})
        return {
            "ambiguity_type": None,
            "clarification_question": None,
            "active_query": active_query,
            "resolved_bindings": res_data.get("resolved_bindings", {}),
        }

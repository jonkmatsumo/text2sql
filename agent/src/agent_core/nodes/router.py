"""Router node for intent classification and ambiguity detection.

This module implements the entry point that:
1. Analyzes the user's natural language query
2. Detects potential ambiguities (schema reference, value, temporal, metric)
3. Routes to clarification or retrieval based on ambiguity detection
"""

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize LLM (temperature=0 for deterministic classification)
llm = get_llm_client(temperature=0)


# Ambiguity taxonomy based on common enterprise data ambiguities
AMBIGUITY_TAXONOMY = {
    "UNCLEAR_SCHEMA_REFERENCE": {
        "description": "Multiple tables contain the referenced column or entity",
        "example": "'region' exists in both Customer and Store tables",
        "question_template": ("Do you mean the {entity} from {option_a} or {option_b}?"),
    },
    "UNCLEAR_VALUE_REFERENCE": {
        "description": "Value exists in multiple contexts",
        "example": "'Fresno' is both a City name and a County name",
        "question_template": "Are you referring to {value} as a {option_a} or {option_b}?",
    },
    "MISSING_TEMPORAL_CONSTRAINT": {
        "description": "Query requires date range but none specified",
        "example": "'Show total sales' without specifying time period",
        "question_template": (
            "Would you like {metric} for the current {period}, " "or a specific date range?"
        ),
    },
    "LOGICAL_METRIC_CONFLICT": {
        "description": "Multiple metrics could satisfy 'top', 'best', 'highest'",
        "example": "'Top customers' could be by spend, frequency, or recency",
        "question_template": "Should '{qualifier}' be calculated by {option_a} or {option_b}?",
    },
    "MISSING_FILTER_CRITERIA": {
        "description": "Query could apply to multiple subsets without filter",
        "example": "'Show orders' without specifying status or category",
        "question_template": "Would you like to see all {entity}, or filter by {filter_options}?",
    },
}


ROUTER_SYSTEM_PROMPT = """You are a query intent analyzer for a Text-to-SQL system.

Your job is to analyze the user's natural language question and determine:
1. Is the query clear enough to generate SQL directly?
2. Does it contain any ambiguities that need clarification?

Ambiguity Types to detect:
- UNCLEAR_SCHEMA_REFERENCE: Column/entity could refer to multiple tables
- UNCLEAR_VALUE_REFERENCE: A value could have multiple meanings
- MISSING_TEMPORAL_CONSTRAINT: Date/time range is needed but not specified
- LOGICAL_METRIC_CONFLICT: Ranking/comparison criteria is ambiguous
- MISSING_FILTER_CRITERIA: Query is too broad without filters

Schema Context (for reference):
{schema_context}

Respond in JSON format:
{{
    "is_ambiguous": true/false,
    "ambiguity_type": "TYPE_NAME or null",
    "ambiguity_reason": "Brief explanation",
    "clarification_question": "Question to ask user (only if ambiguous)",
    "confidence": 0.0-1.0
}}

If the query is clear (confidence > 0.8), set is_ambiguous to false.
Only flag as ambiguous if clarification would meaningfully improve the SQL quality.
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
    with mlflow.start_span(
        name="router",
        span_type=mlflow.entities.SpanType.CHAIN,
    ) as span:
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""
        schema_context = state.get("schema_context", "")

        # Check if we already have clarification (resuming after interrupt)
        user_clarification = state.get("user_clarification")
        if user_clarification:
            # Clear ambiguity and proceed
            span.set_outputs({"action": "proceed_with_clarification"})
            return {
                "ambiguity_type": None,
                "clarification_question": None,
            }

        span.set_inputs({"user_query": user_query})

        if not user_query:
            span.set_outputs({"error": "No query to route"})
            return {}

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", ROUTER_SYSTEM_PROMPT),
                (
                    "user",
                    "Analyze this query for ambiguity: {question}",
                ),
            ]
        )

        chain = prompt | llm

        response = chain.invoke(
            {
                "schema_context": schema_context,
                "question": user_query,
            }
        )

        # Parse JSON response
        import json

        response_text = response.content.strip()
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()

        try:
            analysis = json.loads(response_text)
        except json.JSONDecodeError:
            # Default to non-ambiguous if parsing fails
            analysis = {"is_ambiguous": False, "confidence": 0.5}

        is_ambiguous = analysis.get("is_ambiguous", False)
        confidence = analysis.get("confidence", 1.0)

        span.set_attribute("is_ambiguous", str(is_ambiguous))
        span.set_attribute("confidence", str(confidence))

        if is_ambiguous and confidence < 0.8:
            ambiguity_type = analysis.get("ambiguity_type")
            clarification_question = analysis.get(
                "clarification_question",
                "Could you please clarify your question?",
            )

            span.set_outputs(
                {
                    "action": "clarify",
                    "ambiguity_type": ambiguity_type,
                }
            )

            return {
                "ambiguity_type": ambiguity_type,
                "clarification_question": clarification_question,
            }

        # Query is clear - proceed to retrieval
        span.set_outputs({"action": "retrieve"})
        return {
            "ambiguity_type": None,
            "clarification_question": None,
        }

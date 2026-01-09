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
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

load_dotenv()

# Initialize LLM (temperature=0 for deterministic classification)
llm = get_llm_client(temperature=0)


CONTEXTUALIZE_SYSTEM_PROMPT = """You are a helpful assistant that reformulates follow-up questions.
Given the conversation history and the latest user question, rephrase the latest question into a
standalone question that can be understood without the chat history.
DO NOT answer the question. JUST rephrase it.
If the latest question is already standalone, return it as is.
"""

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


ROUTER_SYSTEM_PROMPT = """You are a Text-to-SQL query intent analyzer.
Your job is to determine if the user's query requires clarification or if it can be unequivocally
resolved using safe defaults.

### 1. Analysis Rules
Analyze the query for specific ambiguities.
- **UNCLEAR_SCHEMA_REFERENCE**: Entity could map to multiple different tables
  (e.g. "Customer" vs "Store").
- **UNCLEAR_VALUE_REFERENCE**: A value has multiple valid interpretations in the schema.
- **MISSING_TEMPORAL_CONSTRAINT**: Metric implies a time window, but none is specified.
- **LOGICAL_METRIC_CONFLICT**: Terms like "top", "best" have multiple valid definitions.
- **MISSING_FILTER_CRITERIA**: Scope is too broad without specific filters.

### 2. Decision Logic
**Do NOT ask clarification questions if:**
- The ambiguity can be resolved by a **Safe Default** (common business assumption, synonym,
  casing). Record this in `assumptions`.
- The question is about formatting, normalization, or style.
- The user has already provided an answer to a similar clarification (Loop Prevention).
- You are >80% confident (confidence > 0.8) in a specific interpretation.

**Only ask clarification questions if:**
- The answer materially changes the SQL structure (joins, grouping, aggregation, metric logic,
  required filters).

**Single-Shot Rule:**
- You may ask **at most one** clarification question per analysis key.
- If the user's response resolves the core ambiguity, mark `resolved_by_user_answer: true`
  and proceed.

### 3. Question Style
- Use natural, non-technical language.
- Discuss business concepts only. DO NOT mention table names, column names, or SQL logic.
- Provide 2-3 concrete options max.
- Do not stack hedges ("if possible... otherwise...").

### 4. Output Schema (JSON)
Respond ONLY with this JSON structure:
{{
    "is_ambiguous": boolean,
    "ambiguity_type": "TYPE_NAME" or null,
    "ambiguity_reason": "Brief explanation",
    "clarification_question": "Question string" or null,
    "assumptions": ["List of safe defaults applied"],
    "resolved_by_user_answer": boolean,
    "confidence": 0.0 to 1.0
}}

### Schema Context
{schema_context}
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
                contextualize_chain = contextualize_prompt | llm

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
            return {
                "ambiguity_type": None,
                "clarification_question": None,
                "active_query": active_query,
            }

        if not active_query:
            span.set_outputs({"error": "No query to route"})
            return {}

        # 3. Ambiguity Detection (on active_query)
        # ---------------------------------------------------------------------
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
                "question": active_query,
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
                "active_query": active_query,
            }

        # Query is clear - proceed to retrieval
        span.set_outputs({"action": "retrieve"})
        return {
            "ambiguity_type": None,
            "clarification_question": None,
            "active_query": active_query,
        }

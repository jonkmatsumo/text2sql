"""SQL-of-Thought Planner node for multi-agent query decomposition.

This module implements the "Plan-Then-Generate" pattern:
1. Schema Linking: Filters relevant tables/columns from retrieval context
2. Subproblem Decomposition: Breaks query into clause-level JSON components
3. Procedural Planning: Generates numbered step-by-step SQL plan
4. Ingredient Validation: Verifies schema has required columns/tables
"""

import json

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys

load_dotenv()


# System prompt for SQL-of-Thought planning
PLANNER_SYSTEM_PROMPT = """You are a SQL planning expert. Your job is to decompose a \
natural language question into a structured execution plan BEFORE any SQL is written.

Given a user question and schema context, you must:
1. Identify which tables and columns are STRICTLY NECESSARY
2. Break down the query into logical clauses (FROM, JOIN, WHERE, GROUP BY, etc.)
3. Create a step-by-step procedural plan for the SQL query
4. List the required "ingredients" (columns/tables) that must exist in the schema

IMPORTANT: Think like a senior data engineer - plan first, then implement.

Output your plan in the following JSON format:
{{
    "schema_linking": {{
        "relevant_tables": ["table1", "table2"],
        "relevant_columns": ["table1.col1", "table2.col2"],
        "reasoning": "Brief explanation of why these are needed"
    }},
    "clause_map": {{
        "from": ["primary_table"],
        "joins": [
            {{"type": "JOIN", "table": "secondary_table", "on": "condition"}}
        ],
        "where": ["condition1", "condition2"],
        "group_by": ["column1"],
        "having": [],
        "order_by": ["column DESC"],
        "limit": null,
        "aggregations": ["COUNT(*)", "SUM(amount)"]
    }},
    "procedural_plan": [
        "Step 1: Start with the primary table X",
        "Step 2: Join with table Y on shared key",
        "Step 3: Apply filter for condition Z",
        "Step 4: Group results by category",
        "Step 5: Order by descending count"
    ],
    "schema_ingredients": [
        "table1.required_column",
        "table2.join_key"
    ],
    "complexity_assessment": {{
        "requires_joins": true,
        "requires_aggregation": true,
        "requires_subquery": false,
        "requires_window_function": false,
        "estimated_difficulty": "medium"
    }}
}}

If the schema is missing required ingredients, include:
{{
    "missing_ingredients": ["table.column that is needed but not found"],
    "recommendation": "Suggest how to proceed or what to ask the user"
}}
"""


async def plan_sql_node(state: AgentState) -> dict:
    """
    Node: PlanSQL.

    Decomposes complex queries into logical steps before SQL synthesis.
    This enables:
    - Better schema linking (reduced hallucination)
    - Procedural verification (each step grounded in schema)
    - Chain-of-thought reasoning for complex queries

    Args:
        state: Current agent state with messages and schema_context

    Returns:
        dict: Updated state with procedural_plan, clause_map, and schema_ingredients
    """
    with telemetry.start_span(
        name="plan_sql",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "plan_sql")
        messages = state["messages"]
        schema_context = state.get("schema_context", "")
        # Use active_query if available, otherwise fallback to last message + clarification
        user_query = state.get("active_query")
        if not user_query:
            user_query = messages[-1].content if messages else ""

            # Include any user clarification if available
            user_clarification = state.get("user_clarification")
            if user_clarification:
                user_query = f"{user_query}\n\nUser Clarification: {user_clarification}"

        tenant_id = state.get("tenant_id")
        interaction_id = state.get("interaction_id")

        if tenant_id:
            span.set_attribute("tenant_id", tenant_id)
        if interaction_id:
            span.set_attribute("interaction_id", interaction_id)

        span.set_inputs(
            {
                "user_query": user_query,
                "schema_context_length": len(schema_context),
            }
        )

        if not user_query:
            span.set_outputs({"error": "No query to plan"})
            return {}

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", PLANNER_SYSTEM_PROMPT),
                (
                    "user",
                    """Schema Context:
{schema_context}

User Question: {question}

Generate the SQL execution plan:""",
                ),
            ]
        )

        from agent.llm_client import get_llm

        chain = prompt | get_llm(temperature=0)

        response = chain.invoke(
            {
                "schema_context": schema_context,
                "question": user_query,
            }
        )

        # Capture token usage
        from agent.llm_client import extract_token_usage

        usage_stats = extract_token_usage(response)
        if usage_stats:
            span.set_attributes(usage_stats)

        # Parse the JSON response
        plan_text = response.content.strip()

        # Extract JSON from response (handle markdown code blocks)
        if "```json" in plan_text:
            plan_text = plan_text.split("```json")[1].split("```")[0].strip()
        elif "```" in plan_text:
            plan_text = plan_text.split("```")[1].split("```")[0].strip()

        try:
            plan_data = json.loads(plan_text)
        except json.JSONDecodeError:
            # Fallback: create minimal plan from raw response
            span.set_attribute("parse_error", "true")
            plan_data = {
                "procedural_plan": [plan_text],
                "clause_map": {},
                "schema_ingredients": [],
            }

        # Extract plan components
        procedural_plan = plan_data.get("procedural_plan", [])
        if isinstance(procedural_plan, list):
            procedural_plan = "\n".join(procedural_plan)

        clause_map = plan_data.get("clause_map", {})
        schema_ingredients = plan_data.get("schema_ingredients", [])

        # Check for missing ingredients
        missing_ingredients = plan_data.get("missing_ingredients", [])
        if missing_ingredients:
            span.set_attribute("missing_ingredients", str(missing_ingredients))

        span.set_outputs(
            {
                "plan_steps": len(plan_data.get("procedural_plan", [])),
                "tables_identified": len(
                    plan_data.get("schema_linking", {}).get("relevant_tables", [])
                ),
                "has_missing_ingredients": len(missing_ingredients) > 0,
            }
        )

        return {
            "procedural_plan": procedural_plan,
            "clause_map": clause_map,
            "schema_ingredients": schema_ingredients,
        }

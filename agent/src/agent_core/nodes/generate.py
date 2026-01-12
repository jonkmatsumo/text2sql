"""SQL generation node using LLM with RAG context, few-shot learning, and semantic caching."""

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize LLM using the factory (temperature=0 for deterministic SQL generation)
llm = get_llm_client(temperature=0)


async def get_few_shot_examples(user_query: str, tenant_id: int = 1) -> str:
    """
    Retrieve relevant few-shot examples via the recommendation service.

    Args:
        user_query: The user's natural language question
        tenant_id: Tenant ID for isolation

    Returns:
        Formatted string with examples, or empty string if none found
    """
    from agent_core.tools import get_mcp_tools

    tools = await get_mcp_tools()
    if not tools:
        return ""

    # Prefer 'recommend_examples' over the legacy 'get_few_shot_examples'
    recommend_tool = next((t for t in tools if t.name == "recommend_examples"), None)
    legacy_tool = next((t for t in tools if t.name == "get_few_shot_examples"), None)

    selected_tool = recommend_tool or legacy_tool
    if not selected_tool:
        return ""

    try:
        # recommend_examples uses 'query', get_few_shot_examples uses 'query'
        result = await selected_tool.ainvoke(
            {"query": user_query, "tenant_id": tenant_id, "limit": 3}
        )
        if not result:
            return ""

        from agent_core.utils.parsing import parse_tool_output

        output = parse_tool_output(result)

        # parse_tool_output returns a list of chunks.
        # recommend_examples returns a dict with 'examples'.
        # legacy returns a flat list of dicts.
        if (
            output
            and isinstance(output, list)
            and isinstance(output[0], dict)
            and "examples" in output[0]
        ):
            examples = output[0]["examples"]
        else:
            examples = output

        formatted = []
        for ex in examples:
            if isinstance(ex, dict):
                # Both structures have question/sql or question/sql_query
                q = ex.get("question")
                s = ex.get("sql") or ex.get("sql_query")
                if q and s:
                    formatted.append(f"- Question: {q}\n  SQL: {s}")

        return "\n\n".join(formatted)

    except Exception as e:
        print(f"Warning: Could not retrieve few-shot examples: {e}")


async def generate_sql_node(state: AgentState) -> dict:
    """
    Node 2: GenerateSQL.

    Checks cache first, then synthesizes executable SQL from the retrieved context,
    few-shot examples, and user question if cache miss.

    Args:
        state: Current agent state with schema_context, messages, and optional tenant_id

    Returns:
        dict: Updated state with current_sql populated and from_cache flag
    """
    with mlflow.start_span(
        name="generate_sql",
        span_type=mlflow.entities.SpanType.CHAT_MODEL,
    ) as span:
        messages = state["messages"]
        context = state.get("schema_context", "")

        # Use active_query if available, else fallback
        active_query = state.get("active_query")
        if active_query:
            user_query = active_query
        else:
            user_query = messages[-1].content if messages else ""
        tenant_id = state.get("tenant_id")

        span.set_inputs(
            {
                "user_query": user_query,
                "context_length": len(context),
                "tenant_id": tenant_id,
            }
        )

        span.set_attribute("cache_hit", "false")

        # Cache miss - proceed with normal generation
        # Retrieve few-shot examples
        few_shot_examples = ""
        try:
            few_shot_examples = await get_few_shot_examples(user_query, tenant_id or 1)
        except Exception as e:
            print(f"Warning: Could not retrieve few-shot examples: {e}")

        # Use schema_context directly from retrieve node (now powered by semantic subgraph)
        # No need for redundant get_table_schema call - graph already contains full schema
        schema_context_to_use = context

        # Build system prompt with examples section
        # NOTE: We must escape curly braces in all injected content because ChatPromptTemplate
        # treats them as variables. {{ becomes { and }} becomes }.

        escaped_examples = (
            few_shot_examples.replace("{", "{{").replace("}", "}}") if few_shot_examples else ""
        )
        examples_section = (
            f"\n\n### Reference Examples\n{escaped_examples}"
            if escaped_examples
            else "\n\n### Reference Examples\nNo examples available."
        )

        # Include procedural plan if available (from SQL-of-Thought planner)
        procedural_plan_raw = state.get("procedural_plan", "")
        clause_map = state.get("clause_map", {})
        user_clarification_raw = state.get("user_clarification", "")

        plan_section = ""
        if procedural_plan_raw:
            # Escape braces in procedural plan
            escaped_plan = procedural_plan_raw.replace("{", "{{").replace("}", "}}")
            plan_section = f"""

PROCEDURAL PLAN (follow this step-by-step):
{escaped_plan}
"""
            if clause_map:
                import json

                # Escape braces in JSON
                clause_map_str = (
                    json.dumps(clause_map, indent=2).replace("{", "{{").replace("}", "}}")
                )
                plan_section += f"""
CLAUSE MAP:
{clause_map_str}
"""

        clarification_section = ""
        if user_clarification_raw:
            # Escape braces in clarification
            escaped_clarification = user_clarification_raw.replace("{", "{{").replace("}", "}}")
            clarification_section = f"""

USER CLARIFICATION:
{escaped_clarification}
"""

        rejected_cache = state.get("rejected_cache_context")
        rejected_cache_section = ""
        if rejected_cache and isinstance(rejected_cache, dict):
            rejected_sql = rejected_cache.get("sql")
            rejected_query = rejected_cache.get("original_query", "Unknown")
            rejection_reason = rejected_cache.get("reason", "Structural mismatch")

            rejected_sql = (
                rejected_sql.replace("{", "{{").replace("}", "}}") if rejected_sql else ""
            )
            rejected_query = (
                rejected_query.replace("{", "{{").replace("}", "}}") if rejected_query else ""
            )
            rejection_reason = (
                rejection_reason.replace("{", "{{").replace("}", "}}") if rejection_reason else ""
            )

            rejected_cache_section = f"""
[HINT FROM REJECTED CACHE]
The following SQL was generated for a similar question ("{rejected_query}") but was rejected.
Rejected SQL: {rejected_sql}
Rejection Reason: {rejection_reason}
Insight: The rejected SQL might be structurally correct but uses wrong entities.
Use it as a template but correct the entities.
"""

        system_prompt = f"""You are a PostgreSQL expert.
Using the provided SCHEMA CONTEXT, PROCEDURAL PLAN, and REFERENCE EXAMPLES, synthesize a SQL query.
The SCHEMA CONTEXT below is a Relationship Graph showing tables, columns, and foreign keys.

Rules:
- Return ONLY the SQL query. No markdown, no explanations.
- Always limit results to 1000 rows unless the user specifies otherwise.
- Use proper SQL syntax for PostgreSQL.
- Only use tables and columns explicitly defined in the Relationship Graph.
- Refer to the **Reference Examples** section below to understand the preferred query style
  and column naming conventions.
- FOLLOW THE PROCEDURAL PLAN if provided - it contains the logical steps.
{plan_section}{clarification_section}{rejected_cache_section}
{{schema_context}}
{examples_section}
"""

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                (
                    "user",
                    "Question: {question}",
                ),
            ]
        )

        chain = prompt | llm

        # Generate SQL (MLflow autolog will capture token usage)
        response = chain.invoke(
            {
                "schema_context": schema_context_to_use,
                "question": user_query,
            }
        )

        # Extract SQL from response (remove markdown code blocks if present)
        sql = response.content.strip()
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()

        span.set_outputs(
            {
                "sql": sql,
                "from_cache": False,
            }
        )

        return {
            "current_sql": sql,
            "from_cache": False,
        }

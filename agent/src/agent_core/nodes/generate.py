"""SQL generation node using LLM with RAG context, few-shot learning, and semantic caching."""

from typing import Optional

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Enable MLflow autolog for OpenAI
mlflow.openai.autolog()

# Initialize LLM using the factory (temperature=0 for deterministic SQL generation)
llm = get_llm_client(temperature=0)


async def check_cache(user_query: str, tenant_id: Optional[int] = None) -> Optional[str]:
    """
    Check semantic cache for similar query.

    Args:
        user_query: The user's natural language question
        tenant_id: Optional tenant ID (required for cache lookup)

    Returns:
        Cached SQL if found, None otherwise
    """
    if not tenant_id:
        return None

    try:
        import json

        from agent_core.tools import get_mcp_tools

        tools = await get_mcp_tools()
        if not tools:
            return None

        # Find the lookup_cache_tool
        cache_tool = None
        for tool in tools:
            if tool.name == "lookup_cache_tool":
                cache_tool = tool
                break

        if not cache_tool:
            return None

        # Call the tool
        result = await cache_tool.ainvoke({"user_query": user_query})
        if isinstance(result, str):
            parsed = json.loads(result)
            return parsed.get("sql")
        return None
    except Exception as e:
        print(f"Warning: Cache lookup failed: {e}")
        return None


async def get_few_shot_examples(user_query: str) -> str:
    """
    Retrieve relevant few-shot examples via MCP server.

    Args:
        user_query: The user's natural language question

    Returns:
        Formatted string with examples, or empty string if none found
    """
    from agent_core.tools import get_mcp_tools

    # Get MCP tools
    tools = await get_mcp_tools()
    if not tools:
        return ""

    # Find the get_few_shot_examples_tool
    few_shot_tool = None
    for tool in tools:
        if tool.name == "get_few_shot_examples_tool":
            few_shot_tool = tool
            break

    if not few_shot_tool:
        return ""

    try:
        # Call the tool
        result = await few_shot_tool.ainvoke({"user_query": user_query, "limit": 3})
        return result if isinstance(result, str) else ""
    except Exception as e:
        print(f"Warning: Could not retrieve few-shot examples: {e}")
        return ""


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
        span_type="CHAT_MODEL",
    ) as span:
        messages = state["messages"]
        context = state.get("schema_context", "")
        user_query = messages[-1].content if messages else ""
        tenant_id = state.get("tenant_id")

        span.set_inputs(
            {
                "user_query": user_query,
                "context_length": len(context),
                "tenant_id": tenant_id,
            }
        )

        # Check cache first (before generating SQL)
        cached_sql = None
        if tenant_id:
            try:
                cached_sql = await check_cache(user_query, tenant_id)
            except Exception as e:
                print(f"Warning: Cache check failed: {e}")

        if cached_sql:
            span.set_attribute("cache_hit", "true")
            span.set_outputs(
                {
                    "sql": cached_sql,
                    "from_cache": True,
                }
            )
            print("✓ Using cached SQL")
            return {
                "current_sql": cached_sql,
                "from_cache": True,
            }

        span.set_attribute("cache_hit", "false")

        # Cache miss - proceed with normal generation
        # Retrieve few-shot examples
        few_shot_examples = ""
        try:
            few_shot_examples = await get_few_shot_examples(user_query)
        except Exception as e:
            print(f"Warning: Could not retrieve few-shot examples: {e}")

        # Fetch Live Schema DDL for identified tables
        table_names = state.get("table_names", [])
        live_schema_ddl = ""
        if table_names:
            try:
                from agent_core.tools import get_mcp_tools

                tools = await get_mcp_tools()
                schema_tool = next((t for t in tools if t.name == "get_table_schema_tool"), None)
                if schema_tool:
                    print(f"Fetching schema for: {table_names}")
                    # Fetch live schema
                    kwargs = {"table_names": table_names}
                    live_schema_ddl = await schema_tool.ainvoke(kwargs)
            except Exception as e:
                print(f"Warning: Failed to fetch live schema: {e}")

        # Use Live DDL if available, otherwise fallback to context (Summary) + Warning?
        # Actually context only has summaries now, so mapped to DDL is critical.
        schema_context_to_use = live_schema_ddl if live_schema_ddl else context

        # Build system prompt with examples section
        examples_section = (
            f"\n\n{few_shot_examples}" if few_shot_examples else "\n\nNo examples available."
        )

        system_prompt = f"""You are a PostgreSQL expert.
Using the provided SCHEMA CONTEXT and EXAMPLES, generate a SQL query to answer the question.

Rules:
- Return ONLY the SQL query. No markdown, no explanations.
- Always limit results to 1000 rows unless the user specifies otherwise.
- Use proper SQL syntax for PostgreSQL.
- Only use tables and columns explicitly defined in the SCHEMA CONTEXT DDL.
- If the question is ambiguous, make reasonable assumptions and note them.
- Learn from the EXAMPLES provided to understand similar query patterns,
  but prioritize the actual schema DDL.

Schema Context:
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

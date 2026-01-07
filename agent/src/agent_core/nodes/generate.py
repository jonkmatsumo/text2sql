"""SQL generation node using LLM with RAG context, few-shot learning, and semantic caching."""

import asyncio
import os
from typing import Optional

from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

load_dotenv()


# Initialize LLM
llm = ChatOpenAI(
    model=os.getenv("OPENAI_MODEL", "gpt-4o"),
    temperature=0,  # Deterministic SQL generation
)


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


def generate_sql_node(state: AgentState) -> dict:
    """
    Node 3: GenerateSQL.

    Checks cache first, then synthesizes executable SQL from the retrieved context,
    few-shot examples, and user question if cache miss.

    Args:
        state: Current agent state with schema_context, messages, and optional tenant_id

    Returns:
        dict: Updated state with current_sql populated and from_cache flag
    """
    # Extract the last user message
    question = state["messages"][-1].content
    tenant_id = state.get("tenant_id")

    # Check cache first (before generating SQL)
    cached_sql = None
    if tenant_id:
        try:
            cached_sql = asyncio.run(check_cache(question, tenant_id))
        except Exception as e:
            print(f"Warning: Cache check failed: {e}")

    if cached_sql:
        print("✓ Using cached SQL")
        return {
            "current_sql": cached_sql,
            "from_cache": True,
        }

    # Cache miss - proceed with normal generation
    # Retrieve few-shot examples
    few_shot_examples = ""
    try:
        # Use asyncio.run to call async function from sync context
        few_shot_examples = asyncio.run(get_few_shot_examples(question))
    except Exception as e:
        print(f"Warning: Could not retrieve few-shot examples: {e}")

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
- Only use tables and columns mentioned in the SCHEMA CONTEXT.
- If the question is ambiguous, make reasonable assumptions and note them.
- Learn from the EXAMPLES provided to understand similar query patterns.

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

    response = chain.invoke(
        {
            "schema_context": state["schema_context"],
            "question": question,
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

    return {
        "current_sql": sql,
        "from_cache": False,
    }

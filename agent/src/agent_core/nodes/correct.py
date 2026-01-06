"""SQL correction node for self-healing queries."""

import os

from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

load_dotenv()

llm = ChatOpenAI(
    model=os.getenv("OPENAI_MODEL", "gpt-4o"),
    temperature=0,
)


def correct_sql_node(state: AgentState) -> dict:
    """
    Node 5: CorrectSQL.

    Analyzes the error and the previous query to generate a fix.
    This implements the self-correction loop for handling SQL errors.

    Args:
        state: Current agent state with error and current_sql

    Returns:
        dict: Updated state with corrected SQL and incremented retry_count
    """
    error_msg = state["error"]
    bad_query = state["current_sql"]
    retry = state.get("retry_count", 0) + 1
    schema_context = state.get("schema_context", "")

    system_prompt = """You are a PostgreSQL expert.
Fix the SQL query based on the error message.
Return ONLY the corrected SQL query. No markdown, no explanations.
"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            (
                "user",
                """Schema Context:
{schema_context}

Failed Query:
{bad_query}

Error Message:
{error_msg}

Please correct the SQL query.""",
            ),
        ]
    )

    chain = prompt | llm

    response = chain.invoke(
        {
            "schema_context": schema_context,
            "bad_query": bad_query,
            "error_msg": error_msg,
        }
    )

    # Extract SQL from response (remove markdown code blocks if present)
    corrected_sql = response.content.strip()
    if corrected_sql.startswith("```sql"):
        corrected_sql = corrected_sql[6:]
    if corrected_sql.startswith("```"):
        corrected_sql = corrected_sql[3:]
    if corrected_sql.endswith("```"):
        corrected_sql = corrected_sql[:-3]
    corrected_sql = corrected_sql.strip()

    return {
        "current_sql": corrected_sql,
        "retry_count": retry,
        "error": None,  # Reset error for next attempt
    }

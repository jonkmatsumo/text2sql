"""SQL correction node for self-healing queries with MLflow tracing."""

import os

import mlflow
from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

load_dotenv()

llm = ChatOpenAI(
    model=os.getenv("OPENAI_MODEL", "gpt-5.2"),
    temperature=0,
)


def correct_sql_node(state: AgentState) -> dict:
    """
    Node 4: CorrectSQL.

    Analyzes the error and the previous query to generate a fix.
    This implements the self-correction loop for handling SQL errors.

    Args:
        state: Current agent state with error and current_sql

    Returns:
        dict: Updated state with corrected SQL and incremented retry_count
    """
    with mlflow.start_span(
        name="correct_sql",
        span_type=mlflow.entities.SpanType.CHAIN,
    ) as span:
        error = state.get("error")
        current_sql = state.get("current_sql")
        schema_context = state.get("schema_context", "")
        retry_count = state.get("retry_count", 0)

        span.set_inputs(
            {
                "error": error,
                "current_sql": current_sql,
                "retry_count": retry_count,
            }
        )

        retry = retry_count + 1

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
                "bad_query": current_sql,
                "error_msg": error,
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

        span.set_outputs(
            {
                "corrected_sql": corrected_sql,
                "retry_count": retry,
            }
        )

        return {
            "current_sql": corrected_sql,
            "retry_count": retry,
            "error": None,  # Reset error for next attempt
        }

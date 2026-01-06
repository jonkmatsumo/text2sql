"""SQL generation node using LLM with RAG context."""

import os

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


def generate_sql_node(state: AgentState) -> dict:
    """
    Node 3: GenerateSQL.

    Synthesizes executable SQL from the retrieved context and user question.

    Args:
        state: Current agent state with schema_context and messages

    Returns:
        dict: Updated state with current_sql populated
    """
    system_prompt = """You are a PostgreSQL expert.
Using the provided SCHEMA CONTEXT, generate a SQL query to answer the question.

Rules:
- Return ONLY the SQL query. No markdown, no explanations.
- Always limit results to 1000 rows unless the user specifies otherwise.
- Use proper SQL syntax for PostgreSQL.
- Only use tables and columns mentioned in the SCHEMA CONTEXT.
- If the question is ambiguous, make reasonable assumptions and note them.
"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            (
                "user",
                "Schema Context:\n{schema_context}\n\nQuestion: {question}",
            ),
        ]
    )

    chain = prompt | llm

    # Extract the last user message
    question = state["messages"][-1].content

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

    return {"current_sql": sql}

"""Insight synthesis node for formatting results."""

import json

from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from dotenv import load_dotenv
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize LLM using the factory (temperature=0.7 for more creative responses)
llm = get_llm_client(temperature=0.7)


def synthesize_insight_node(state: AgentState) -> dict:
    """
    Node 6: SynthesizeInsight.

    Formats the query result into a natural language response.

    Args:
        state: Current agent state with query_result

    Returns:
        dict: Updated state with synthesized response in messages
    """
    query_result = state["query_result"]

    # Get the original question from the first user message
    original_question = ""
    if state["messages"]:
        original_question = state["messages"][0].content

    if not query_result:
        return {
            "messages": [
                AIMessage(content="I couldn't retrieve any results for your query."),
            ]
        }

    # Format result as JSON string for LLM
    result_str = json.dumps(query_result, indent=2, default=str)

    system_prompt = """You are a helpful data analyst assistant.
Format the query results into a clear, natural language response.
Be concise but informative. Use numbers and data from the results.
"""

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            (
                "user",
                "Question: {question}\n\nQuery Results:\n{results}\n\nProvide a clear answer:",
            ),
        ]
    )

    chain = prompt | llm

    response = chain.invoke(
        {
            "question": original_question,
            "results": result_str,
        }
    )

    return {
        "messages": [AIMessage(content=response.content)],
    }

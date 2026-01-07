"""Testable business logic for Streamlit app.

This module contains all business logic that can be tested independently
of Streamlit UI components. It handles agent integration, state management,
and result processing.
"""

import sys
from pathlib import Path
from typing import Dict, Optional

from langchain_core.messages import HumanMessage

# Add agent to path
sys.path.insert(0, str(Path(__file__).parent.parent / "agent" / "src"))

from agent_core.graph import app  # noqa: E402


async def run_agent(question: str, tenant_id: int) -> Dict:
    """
    Run the agent workflow and return structured results.

    This function is testable without Streamlit dependencies. It handles
    the entire agent workflow execution and extracts relevant information
    from the agent's state transitions.

    Args:
        question: Natural language question from the user
        tenant_id: Tenant identifier for multi-tenant scenarios

    Returns:
        Dictionary containing:
            - sql: Generated SQL query (if any)
            - result: Query results as list of dicts (if successful)
            - response: Natural language response (if any)
            - error: Error message (if any)
            - from_cache: Boolean indicating if SQL came from cache

    Raises:
        Exception: If agent workflow fails unexpectedly
    """
    inputs = {
        "messages": [HumanMessage(content=question)],
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
        "tenant_id": tenant_id,
    }

    results = {
        "sql": None,
        "result": None,
        "response": None,
        "error": None,
        "from_cache": False,
    }

    async for event in app.astream(inputs):
        for node_name, node_output in event.items():
            if "current_sql" in node_output:
                results["sql"] = node_output["current_sql"]
            if "from_cache" in node_output:
                results["from_cache"] = node_output["from_cache"]
            if "query_result" in node_output:
                results["result"] = node_output["query_result"]
            if "error" in node_output and node_output["error"]:
                results["error"] = node_output["error"]
            if node_name == "synthesize" and "messages" in node_output:
                if node_output["messages"]:
                    results["response"] = node_output["messages"][-1].content

    return results


def format_conversation_entry(question: str, results: Dict) -> Dict:
    """
    Format a conversation entry for storage/display.

    Args:
        question: User's question
        results: Results from run_agent()

    Returns:
        Formatted conversation entry dictionary
    """
    return {
        "question": question,
        "sql": results.get("sql"),
        "result": results.get("result"),
        "response": results.get("response"),
        "error": results.get("error"),
        "from_cache": results.get("from_cache", False),
    }


def validate_tenant_id(tenant_id: Optional[int]) -> int:
    """
    Validate and return tenant ID, defaulting to 1 if None.

    Args:
        tenant_id: Optional tenant ID

    Returns:
        Valid tenant ID (defaults to 1)
    """
    return tenant_id if tenant_id is not None and tenant_id > 0 else 1

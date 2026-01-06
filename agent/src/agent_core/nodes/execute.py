"""SQL execution and validation node."""

import json

from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools


async def validate_and_execute_node(state: AgentState) -> dict:
    """
    Node 4: ValidateAndExecute.

    Calls the 'execute_sql_query' MCP tool to execute the generated SQL.
    The MCP server handles regex security checks internally.

    Args:
        state: Current agent state with current_sql populated

    Returns:
        dict: Updated state with query_result or error
    """
    tools = await get_mcp_tools()
    executor_tool = next((t for t in tools if t.name == "execute_sql_query"), None)

    if not executor_tool:
        return {
            "error": "execute_sql_query tool not found in MCP server",
            "query_result": None,
        }

    query = state["current_sql"]

    if not query:
        return {
            "error": "No SQL query to execute",
            "query_result": None,
        }

    try:
        # The MCP server handles the regex security checks internally
        result = await executor_tool.ainvoke({"sql_query": query})

        # Check if the tool returned a database error string
        if isinstance(result, str):
            if "Error:" in result or "Database Error:" in result:
                return {"error": result, "query_result": None}

            # Try to parse as JSON (successful query result)
            try:
                parsed_result = json.loads(result)
                return {"query_result": parsed_result, "error": None}
            except json.JSONDecodeError:
                # If not JSON, treat as error message
                return {"error": result, "query_result": None}

        # If result is already a dict/list, use it directly
        return {"query_result": result, "error": None}

    except Exception as e:
        return {"error": str(e), "query_result": None}

"""SQL execution and validation node with MLflow tracing."""

import json

import mlflow
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools


async def validate_and_execute_node(state: AgentState) -> dict:
    """
    Node 3: ValidateAndExecute.

    Calls the 'execute_sql_query' MCP tool to execute the generated SQL.
    The MCP server handles regex security checks internally.

    Args:
        state: Current agent state with current_sql populated

    Returns:
        dict: Updated state with query_result or error
    """
    with mlflow.start_span(
        name="execute_sql",
        span_type=mlflow.entities.SpanType.TOOL,
    ) as span:
        sql_query = state.get("current_sql")
        tenant_id = state.get("tenant_id")

        span.set_inputs(
            {
                "sql": sql_query,
                "tenant_id": tenant_id,
            }
        )

        if not sql_query:
            span.set_outputs({"error": "No SQL query to execute"})
            return {
                "error": "No SQL query to execute",
                "query_result": None,
            }

        try:
            tools = await get_mcp_tools()
            executor_tool = next((t for t in tools if t.name == "execute_sql_query_tool"), None)

            if not executor_tool:
                error = "execute_sql_query_tool not found in MCP server"
                span.set_outputs({"error": error})
                return {
                    "error": error,
                    "query_result": None,
                }

            # The MCP server handles the regex security checks internally
            # Note: tenant_id is extracted from context by the MCP server, not passed as a parameter
            result = await executor_tool.ainvoke(
                {
                    "sql_query": sql_query,
                }
            )

            # Check if the tool returned a database error string
            if isinstance(result, str):
                if "Error:" in result or "Database Error:" in result:
                    span.set_outputs({"error": result})
                    return {"error": result, "query_result": None}

                # Try to parse as JSON (successful query result)
                try:
                    parsed_result = json.loads(result)
                    query_result = parsed_result
                    error = None
                except json.JSONDecodeError:
                    # If not JSON, treat as error message
                    span.set_outputs({"error": result})
                    return {"error": result, "query_result": None}
            else:
                # If result is already a dict/list, use it directly
                query_result = result
                error = None

            span.set_outputs(
                {
                    "result_count": len(query_result) if query_result else 0,
                    "success": True,
                }
            )

            # Cache successful SQL generation (if not from cache and tenant_id exists)
            from_cache = state.get("from_cache", False)
            if not error and query_result and sql_query and tenant_id and not from_cache:
                try:
                    # Get cache update tool
                    cache_tool = next((t for t in tools if t.name == "update_cache"), None)
                    if cache_tool:
                        # Extract user query from first message
                        user_query = state["messages"][0].content if state["messages"] else ""
                        if user_query:
                            await cache_tool.ainvoke(
                                {
                                    "user_query": user_query,
                                    "sql": sql_query,
                                }
                            )
                except Exception as e:
                    print(f"Warning: Cache update failed: {e}")

            return {"query_result": query_result, "error": error}

        except Exception as e:
            error = str(e)
            span.set_outputs({"error": error})
            return {
                "error": error,
                "query_result": None,
            }

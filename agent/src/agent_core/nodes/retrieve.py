"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import mlflow
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools


async def retrieve_context_node(state: AgentState) -> dict:
    """
    Node 1: RetrieveContext.

    Queries vector store via MCP server for relevant tables.
    Uses the search_relevant_tables_tool.

    Args:
        state: Current agent state containing conversation messages

    Returns:
        dict: Updated state with schema_context and table_names populated
    """
    with mlflow.start_span(
        name="retrieve_context",
        span_type=mlflow.entities.SpanType.RETRIEVER,
    ) as span:
        # Extract query: use active_query from state (set by router), or fallback
        active_query = state.get("active_query")
        if not active_query:
            messages = state["messages"]
            active_query = messages[-1].content if messages else ""

        span.set_inputs({"user_query": active_query})

        context_str = ""
        table_names = []

        try:
            tools = await get_mcp_tools()
            search_tool = next((t for t in tools if t.name == "search_relevant_tables_tool"), None)

            if search_tool:
                # Call the tool
                # Output format: JSON list of dicts with 'table_name', 'columns', etc.
                context_json = await search_tool.ainvoke({"user_query": active_query, "limit": 5})

                try:
                    from agent_core.utils.parsing import parse_tool_output

                    results = parse_tool_output(context_json)

                    context_parts = []
                    for item in results:
                        # Handle case where item might be a TextContent object
                        # or similar if using weird transport
                        if not isinstance(item, dict):
                            # Fallback or log? Trying to proceed if it looks like what we expect
                            if hasattr(item, "model_dump"):
                                item = item.model_dump()
                            else:
                                continue

                        t_name = item.get("table_name")
                        if not t_name:
                            continue

                        if t_name not in table_names:
                            table_names.append(t_name)

                        # Format for LLM context
                        description = item.get("description", "")
                        columns = item.get("columns", [])

                        table_str = f"Table: {t_name}\nDescription: {description}\nColumns:\n"
                        for col in columns:
                            req = "REQUIRED" if col.get("required") else "NULLABLE"
                            table_str += f"- {col['name']} ({col['type']}, {req})\n"

                        context_parts.append(table_str)

                    if not context_parts:
                        context_str = "No relevant tables found."
                    else:
                        context_str = "\n---\n".join(context_parts)

                except Exception as e:
                    print(
                        f"Error parsing search tool output: {e}, "
                        f"Content: {str(context_json)[:100]}..."
                    )
                    context_str = f"Error retrieving context: {e}"
            else:
                print("Warning: search_relevant_tables_tool not found.")

        except Exception as e:
            print(f"Error during retrieval: {e}")
            context_str = f"Error retrieving context: {e}"

        span.set_outputs(
            {
                "context_length": len(context_str),
                "tables_retrieved": len(table_names),
                "table_names": table_names,
            }
        )

        return {"schema_context": context_str, "table_names": table_names}

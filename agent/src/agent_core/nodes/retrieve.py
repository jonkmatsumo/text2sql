"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import re

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
                # Output format: Markdown with "### {table_name} (similarity: ...)"
                context_str = await search_tool.ainvoke({"user_query": active_query, "limit": 5})

                if isinstance(context_str, str):
                    # Parse table names
                    # Matches "### table_name (similarity:" or just "### table_name"
                    matches = re.finditer(r"###\s+([a-zA-Z0-9_]+)", context_str)
                    for match in matches:
                        name = match.group(1)
                        if (
                            name not in table_names and name != "Relevant"
                        ):  # Avoid "Relevant Tables" header
                            table_names.append(name)
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

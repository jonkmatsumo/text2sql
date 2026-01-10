"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import mlflow
from agent_core.state import AgentState
from agent_core.tools import get_mcp_tools
from agent_core.utils.graph_formatter import format_graph_to_markdown


async def retrieve_context_node(state: AgentState) -> dict:
    """Retrieve schema context using semantic subgraph search.

    Queries Memgraph via MCP server for relevant tables and relationships.
    Uses the get_semantic_subgraph_tool for graph-based retrieval.

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
        cached_sql = None
        cache_metadata = None

        try:
            tools = await get_mcp_tools()
            subgraph_tool = next((t for t in tools if t.name == "get_semantic_subgraph_tool"), None)
            cache_tool = next((t for t in tools if t.name == "lookup_cache_tool"), None)

            # Parallel execution of subgraph retrieval and cache lookup
            import asyncio

            async def fetch_subgraph():
                if not subgraph_tool:
                    return None
                return await subgraph_tool.ainvoke({"query": active_query})

            async def fetch_cache():
                if not cache_tool:
                    return None
                # Retrieve full cache object (with metadata)
                return await cache_tool.ainvoke({"user_query": active_query})

            # Execute both tasks
            subgraph_json, cache_json = await asyncio.gather(fetch_subgraph(), fetch_cache())

            # Process Subgraph Result
            if subgraph_json:
                try:
                    from agent_core.utils.parsing import parse_tool_output

                    graph_data = parse_tool_output(subgraph_json)

                    # Handle case where graph_data is a list with single dict
                    if isinstance(graph_data, list) and len(graph_data) > 0:
                        graph_data = graph_data[0]

                    if isinstance(graph_data, dict):
                        # Extract table names from nodes
                        nodes = graph_data.get("nodes", [])
                        for node in nodes:
                            if node.get("type") == "Table":
                                t_name = node.get("name")
                                if t_name and t_name not in table_names:
                                    table_names.append(t_name)

                        # Format graph to Markdown for LLM consumption
                        context_str = format_graph_to_markdown(graph_data)

                        if not context_str.strip():
                            context_str = "No relevant tables found."
                    else:
                        context_str = "No relevant tables found."

                except Exception as e:
                    print(
                        f"Error parsing subgraph tool output: {e}, "
                        f"Content: {str(subgraph_json)[:100]}..."
                    )
                    context_str = f"Error retrieving context: {e}"
            else:
                if not subgraph_tool:
                    print("Warning: get_semantic_subgraph_tool not found.")
                context_str = "Schema retrieval tool not available." or "No relevant tables found."

            # Process Cache Result
            if cache_json:
                try:
                    from agent_core.utils.parsing import parse_tool_output

                    # Expected format:
                    # {"sql": ..., "original_query": ..., "similarity": ..., "metadata": ...}
                    cache_data = parse_tool_output(cache_json)

                    # Handle potential list wrapping
                    if isinstance(cache_data, list) and len(cache_data) > 0:
                        cache_data = cache_data[0]

                    if isinstance(cache_data, dict) and cache_data.get("sql"):
                        cached_sql = cache_data.get("sql")
                        cache_metadata = cache_data.get("metadata", {})
                        # Ensure we capture original query if not in metadata but in root
                        if not cache_metadata.get("user_query") and cache_data.get(
                            "original_query"
                        ):
                            cache_metadata["user_query"] = cache_data.get("original_query")

                except Exception as e:
                    print(f"Error parsing cache tool output: {e}")

        except Exception as e:
            print(f"Error during retrieval: {e}")
            context_str = f"Error retrieving context: {e}"

        span.set_outputs(
            {
                "context_length": len(context_str),
                "tables_retrieved": len(table_names),
                "table_names": table_names,
                "cache_hit": bool(cached_sql),
            }
        )

        return {
            "schema_context": context_str,
            "table_names": table_names,
            "cached_sql": cached_sql,
            "cache_metadata": cache_metadata,
        }

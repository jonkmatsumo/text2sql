"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import logging

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.tools import get_mcp_tools
from agent.utils.graph_formatter import format_graph_to_markdown

logger = logging.getLogger(__name__)


async def retrieve_context_node(state: AgentState) -> dict:
    """Retrieve schema context using semantic subgraph search.

    Queries Memgraph via MCP server for relevant tables and relationships.
    Uses the get_semantic_subgraph for graph-based retrieval.

    Args:
        state: Current agent state containing conversation messages

    Returns:
        dict: Updated state with schema_context and table_names populated
    """
    with telemetry.start_span(
        name="retrieve_context",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "retrieve_context")
        # Extract query: use active_query from state (set by router), or fallback
        active_query = state.get("active_query")
        if not active_query:
            messages = state["messages"]
            active_query = messages[-1].content if messages else ""

        span.set_inputs({"user_query": active_query})

        # === Pre-retrieval grounding ===
        # Apply canonicalization to ground synonyms (e.g., "customers" → "users")
        # This enriches the query with schema hints before semantic search
        from agent.utils.grounding import extract_schema_hints

        grounded_query, mappings = extract_schema_hints(active_query)
        canonicalization_applied = len(mappings) > 0

        # === Grounding Telemetry (Phase C) ===
        span.set_attribute("grounding.canonicalization_applied", canonicalization_applied)
        span.set_attribute("grounding.schema_hints_count", len(mappings))
        if canonicalization_applied:
            span.set_attribute("grounding.grounded_query", grounded_query)

        context_str = ""
        table_names = []
        graph_data = {}

        try:
            tools = await get_mcp_tools()
            subgraph_tool = next((t for t in tools if t.name == "get_semantic_subgraph"), None)

            if subgraph_tool:
                # Execute subgraph retrieval with grounded query
                payload = {"query": grounded_query}
                tenant_id = state.get("tenant_id")
                if tenant_id is not None:
                    payload["tenant_id"] = tenant_id
                subgraph_json = await subgraph_tool.ainvoke(payload)

                if subgraph_json:
                    try:
                        from agent.utils.parsing import parse_tool_output

                        graph_data = parse_tool_output(subgraph_json)

                        logger.debug("Subgraph output type: %s", type(subgraph_json).__name__)
                        logger.debug("Parsed subgraph output type: %s", type(graph_data).__name__)
                        if isinstance(graph_data, list):
                            logger.debug("Parsed subgraph list size: %d", len(graph_data))

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
                        logger.exception("Error parsing subgraph tool output")
                        context_str = f"Error retrieving context: {e}"
                else:
                    context_str = "No relevant tables found."
            else:
                logger.warning("get_semantic_subgraph tool not found.")
                context_str = "Schema retrieval tool not available."

        except Exception as e:
            logger.exception("Error during retrieval")
            context_str = f"Error retrieving context: {e}"

        span.set_outputs(
            {
                "context_length": len(context_str),
                "tables_retrieved": len(table_names),
                "table_names": table_names,
            }
        )

        raw_nodes = graph_data.get("nodes", []) if isinstance(graph_data, dict) else []
        from agent.utils.schema_fingerprint import resolve_schema_snapshot_id

        schema_snapshot_id = resolve_schema_snapshot_id(raw_nodes)

        logger.info(
            "Schema retrieval completed",
            extra={
                "interaction_id": state.get("interaction_id"),
                "schema_source": "semantic_subgraph",
                "tables_retrieved": len(table_names),
                "nodes_retrieved": len(raw_nodes),
            },
        )

        return {
            "schema_context": context_str,
            "raw_schema_context": raw_nodes,
            "table_names": table_names,
            "schema_snapshot_id": schema_snapshot_id,
        }

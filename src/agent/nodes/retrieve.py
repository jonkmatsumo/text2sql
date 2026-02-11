"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import logging
import time

from agent.models.termination import TerminationReason
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
        stage_start = time.monotonic()

        def _latency_payload() -> dict:
            latency_ms = max(0.0, (time.monotonic() - stage_start) * 1000.0)
            span.set_attribute("latency.retrieval_ms", latency_ms)
            return {"latency_retrieval_ms": latency_ms}

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

        existing_snapshot_id = state.get("schema_snapshot_id")

        try:
            tools = await get_mcp_tools()
            subgraph_tool = next((t for t in tools if t.name == "get_semantic_subgraph"), None)

            if subgraph_tool:
                # Execute subgraph retrieval with grounded query
                payload = {"query": grounded_query}
                if existing_snapshot_id:
                    payload["snapshot_id"] = existing_snapshot_id

                subgraph_json = await subgraph_tool.ainvoke(payload)

                if subgraph_json:
                    try:
                        from agent.utils.parsing import parse_tool_output, unwrap_envelope

                        graph_data = parse_tool_output(subgraph_json)

                        # Handle case where graph_data is a list with single dict or envelope
                        if isinstance(graph_data, list) and len(graph_data) > 0:
                            graph_data = graph_data[0]

                        # Unwrap GenericToolResponseEnvelope if present
                        graph_data = unwrap_envelope(graph_data)

                        if graph_data is None:
                            # This means an error was detected in the envelope (e.g. Unauthorized)
                            context_str = (
                                "Context retrieval failed due to permissions or an internal error. "
                                "Please ensure you have the required roles."
                            )
                            return {
                                "schema_context": context_str,
                                "termination_reason": TerminationReason.PERMISSION_DENIED,
                                **_latency_payload(),
                            }
                        elif isinstance(graph_data, dict):
                            # Extract table names from nodes
                            nodes = graph_data.get("nodes", [])
                            for node in nodes:
                                if node.get("type") == "Table":
                                    # Check for missing/inaccessible status
                                    status = node.get("status")
                                    if status in ("TABLE_NOT_FOUND", "TABLE_INACCESSIBLE"):
                                        logger.warning(f"Table {node.get('name')} is {status}")
                                        span.set_attribute(
                                            f"schema.issue.{node.get('name')}", status
                                        )

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
                context_str = "Context retrieval tool not available."

        except Exception as e:
            logger.exception("Error in retrieve_context_node execution")
            context_str = f"Error in context retrieval: {e}"

        # Drift Detection Logic
        raw_nodes = graph_data.get("nodes", []) if isinstance(graph_data, dict) else []
        from agent.utils.schema_fingerprint import resolve_schema_snapshot_id

        new_snapshot_id = resolve_schema_snapshot_id(raw_nodes)
        refresh_count = state.get("schema_refresh_count", 0)

        if (
            existing_snapshot_id
            and existing_snapshot_id != "unknown"
            and new_snapshot_id != existing_snapshot_id
        ):
            span.set_attribute("schema.drift_detected", True)
            span.set_attribute("schema.old_snapshot_id", existing_snapshot_id)
            span.set_attribute("schema.new_snapshot_id", new_snapshot_id)

            if refresh_count >= 1:
                # Hard fail if already refreshed once
                return {
                    "error": "Schema changed during request and refresh attempt "
                    "failed to stabilize.",
                    "error_category": "schema_changed_during_request",
                    "retry_after_seconds": None,
                    "termination_reason": TerminationReason.SCHEMA_CHANGED,
                    **_latency_payload(),
                }

            # Trigger one-time refresh by clearing context and returning flag
            # (In LangGraph, the router or retrieve node itself can handle this)
            logger.warning("Schema drift detected. Triggering refresh.")
            return {
                "schema_refresh_count": refresh_count + 1,
                "schema_snapshot_id": new_snapshot_id,
                "schema_drift_suspected": True,
                "error": "Schema drift detected",  # Triggers retry logic
                **_latency_payload(),
            }

        logger.info(
            "Schema retrieval completed",
            extra={
                "interaction_id": state.get("interaction_id"),
                "schema_source": "semantic_subgraph",
                "tables_retrieved": len(table_names),
                "nodes_retrieved": len(raw_nodes),
                "snapshot_id": new_snapshot_id,
            },
        )

        return {
            "schema_context": context_str,
            "raw_schema_context": raw_nodes,
            "table_names": table_names,
            "schema_snapshot_id": new_snapshot_id,
            **_latency_payload(),
        }

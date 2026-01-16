import logging

from agent_core.state import AgentState
from agent_core.viz.spec import build_vega_lite_spec

logger = logging.getLogger(__name__)


def visualize_query_node(state: AgentState) -> dict:
    """
    Generate a Vega-Lite visualization specification from the query result.

    This node inspects the 'query_result' in the state. If the data is suitable
    for visualization (based on heuristics), it populates 'viz_spec'.
    Otherwise, it leaves 'viz_spec' as None.

    This is a best-effort node and should never raise exceptions that block the flow.
    """
    logger.info("Entering visualization node")

    from agent_core.telemetry import telemetry
    from agent_core.telemetry_schema import SpanKind, TelemetryKeys

    with telemetry.start_span(
        name="visualize_query",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "visualize_query")

        query_result = state.get("query_result")

        # Simple input logging
        if isinstance(query_result, list):
            span.set_attribute("result_row_count", len(query_result))

        if not query_result or not isinstance(query_result, list):
            span.set_attribute("viz_generated", False)
            return {"viz_spec": None, "viz_reason": "No valid query result to visualize"}

        try:
            # Generate spec
            spec = build_vega_lite_spec(query_result)

            if spec:
                logger.info(f"Generated visualization: {spec.get('mark')} chart")
                span.set_attribute("viz_generated", True)
                span.set_attribute("viz_type", spec.get("mark"))
                return {
                    "viz_spec": spec,
                    "viz_reason": f"Generated {spec.get('mark')} chart based on data shape",
                }
            else:
                logger.info("Data not suitable for visualization")
                span.set_attribute("viz_generated", False)
                return {
                    "viz_spec": None,
                    "viz_reason": (
                        "Data shape not suitable for automatic visualization "
                        "(need 2 cols: cat/num, num/num, date/num)"
                    ),
                }

        except Exception as e:
            logger.error(f"Visualization generation failed: {e}")
            span.set_attribute(TelemetryKeys.ERROR, str(e))
            return {"viz_spec": None, "viz_reason": f"Visualization generation failed: {str(e)}"}

"""SQL validation node for syntactic and semantic correctness with telemetry tracing."""

from typing import Optional, Tuple

import sqlglot
from sqlglot import exp

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.validation.ast_validator import validate_sql


def _extract_limit(sql_query: str) -> Tuple[bool, Optional[int]]:
    try:
        expression = sqlglot.parse_one(sql_query)
    except Exception:
        return False, None

    limit_node = expression.find(exp.Limit)
    top_cls = getattr(exp, "Top", None)
    top_node = expression.find(top_cls) if top_cls else None

    node = limit_node or top_node
    if not node:
        return False, None

    limit_expr = node.args.get("expression")
    if isinstance(limit_expr, exp.Literal) and limit_expr.is_int:
        return True, int(limit_expr.this)

    return True, None


async def validate_sql_node(state: AgentState) -> dict:
    """
    Node: ValidateSQL.

    Runs AST validation on generated SQL before execution.
    Extracts metadata for audit logging and performs security checks.
    TODO(p0): add optional schema-bound validation against raw schema context.

    If validation fails, returns structured error for the healer loop.
    If validation passes, enriches state with metadata for auditing.

    Args:
        state: Current agent state with current_sql populated

    Returns:
        dict: Updated state with validation result and metadata
    """
    with telemetry.start_span(
        name="validate_sql",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "validate_sql")
        sql_query = state.get("current_sql")

        span.set_inputs({"sql": sql_query})

        if not sql_query:
            span.set_outputs({"error": "No SQL query to validate"})
            return {
                "error": "No SQL query to validate",
                "ast_validation_result": None,
            }

        is_limited, limit_value = _extract_limit(sql_query)
        span.set_attribute("result.is_limited", bool(is_limited))
        if limit_value is not None:
            span.set_attribute("result.limit", limit_value)

        # Run AST validation
        result = validate_sql(sql_query)

        # Log validation result
        span.set_attribute("is_valid", str(result.is_valid))
        span.set_attribute("violation_count", str(len(result.violations)))

        if result.metadata:
            span.set_attribute("table_count", str(len(result.metadata.table_lineage)))
            span.set_attribute("join_complexity", str(result.metadata.join_complexity))

        if not result.is_valid:
            # Convert violations to structured error message for healer
            violation_messages = []
            for v in result.violations:
                violation_messages.append(f"[{v.violation_type.value}] {v.message}")

            structured_error = "\n".join(violation_messages)

            span.set_outputs(
                {
                    "is_valid": False,
                    "error": structured_error,
                    "violations": [v.to_dict() for v in result.violations],
                }
            )

            return {
                "error": structured_error,
                "ast_validation_result": result.to_dict(),
                # Preserve metadata even on failure for audit
                "table_lineage": result.metadata.table_lineage if result.metadata else [],
                "column_usage": result.metadata.column_usage if result.metadata else [],
                "join_complexity": result.metadata.join_complexity if result.metadata else 0,
                "result_is_limited": is_limited,
                "result_limit": limit_value,
            }

        # Validation passed - enrich state with metadata
        span.set_outputs(
            {
                "is_valid": True,
                "table_lineage": result.metadata.table_lineage if result.metadata else [],
                "column_usage": result.metadata.column_usage if result.metadata else [],
            }
        )

        return {
            "error": None,  # Clear any previous validation errors
            "ast_validation_result": result.to_dict(),
            "table_lineage": result.metadata.table_lineage if result.metadata else [],
            "column_usage": result.metadata.column_usage if result.metadata else [],
            "join_complexity": result.metadata.join_complexity if result.metadata else 0,
            "result_is_limited": is_limited,
            "result_limit": limit_value,
            # Optionally use normalized SQL
            "current_sql": result.parsed_sql if result.parsed_sql else sql_query,
        }

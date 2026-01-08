"""SQL validation node using AST analysis before execution."""

import mlflow
from agent_core.state import AgentState
from agent_core.validation.ast_validator import validate_sql


async def validate_sql_node(state: AgentState) -> dict:
    """
    Node: ValidateSQL.

    Runs AST validation on generated SQL before execution.
    Extracts metadata for audit logging and performs security checks.

    If validation fails, returns structured error for the healer loop.
    If validation passes, enriches state with metadata for auditing.

    Args:
        state: Current agent state with current_sql populated

    Returns:
        dict: Updated state with validation result and metadata
    """
    with mlflow.start_span(
        name="validate_sql",
        span_type=mlflow.entities.SpanType.CHAIN,
    ) as span:
        sql_query = state.get("current_sql")

        span.set_inputs({"sql": sql_query})

        if not sql_query:
            span.set_outputs({"error": "No SQL query to validate"})
            return {
                "error": "No SQL query to validate",
                "ast_validation_result": None,
            }

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
            # Optionally use normalized SQL
            "current_sql": result.parsed_sql if result.parsed_sql else sql_query,
        }

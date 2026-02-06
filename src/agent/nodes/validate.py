"""SQL validation node for syntactic and semantic correctness with telemetry tracing."""

from typing import Dict, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from agent.state import AgentState
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from agent.validation.ast_validator import validate_sql
from common.config.env import get_env_bool


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


def _build_schema_binding(raw_schema_context: list[dict]) -> Dict[str, Set[str]]:
    tables: Dict[str, Set[str]] = {}
    for node in raw_schema_context:
        if not isinstance(node, dict):
            continue
        if node.get("type") == "Table":
            name = node.get("name")
            if name:
                tables.setdefault(str(name), set())
    for node in raw_schema_context:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "Column":
            continue
        table = node.get("table")
        name = node.get("name")
        if table and name and str(table) in tables:
            tables[str(table)].add(str(name))
    return tables


def _extract_identifiers(sql_query: str) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    try:
        expression = sqlglot.parse_one(sql_query)
    except Exception:
        return set(), set()

    if expression.find(exp.With):
        # Conservative: skip binding when CTEs are present to avoid false positives.
        return set(), set()

    tables = set()
    columns = set()
    for table in expression.find_all(exp.Table):
        if table.db or table.catalog:
            continue
        if table.name:
            tables.add(table.name)
    for column in expression.find_all(exp.Column):
        # Only validate fully qualified columns (table.column) to avoid alias ambiguity.
        if column.table and column.name:
            columns.add((column.table, column.name))
    return tables, columns


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

        schema_binding_enabled = get_env_bool("AGENT_SCHEMA_BINDING_VALIDATION", True) is True
        schema_binding_soft_mode = get_env_bool("AGENT_SCHEMA_BINDING_SOFT_MODE", False) is True
        span.set_attribute("validation.schema_bound_enabled", schema_binding_enabled)
        span.set_attribute("validation.schema_bound_soft_mode", schema_binding_soft_mode)
        if schema_binding_enabled:
            raw_schema_context = state.get("raw_schema_context") or []
            schema_map = (
                _build_schema_binding(raw_schema_context)
                if isinstance(raw_schema_context, list)
                else {}
            )
            if schema_map:
                referenced_tables, referenced_columns = _extract_identifiers(sql_query)
                missing_tables = sorted(t for t in referenced_tables if t not in schema_map)
                missing_columns = sorted(
                    f"{table}.{column}"
                    for table, column in referenced_columns
                    if table in schema_map and column not in schema_map[table]
                )
                span.set_attribute("validation.schema_bound", True)
                span.set_attribute("validation.missing_tables", ",".join(missing_tables[:20]))
                span.set_attribute("validation.missing_columns", ",".join(missing_columns[:20]))
                span.set_attribute("validation.missing_tables_count", len(missing_tables))
                span.set_attribute("validation.missing_columns_count", len(missing_columns))
                if missing_tables or missing_columns:
                    error_parts = []
                    if missing_tables:
                        error_parts.append(f"missing tables: {', '.join(missing_tables)}")
                    if missing_columns:
                        error_parts.append(f"missing columns: {', '.join(missing_columns)}")
                    error_msg = "Schema validation failed; " + "; ".join(error_parts)

                    if schema_binding_soft_mode:
                        # Soft mode: log warning but allow execution to proceed
                        span.set_attribute("validation.schema_bound_blocked", False)
                        span.add_event(
                            "schema_binding.soft_violation",
                            {"message": error_msg, "tables": ",".join(missing_tables[:10])},
                        )
                    else:
                        # Hard mode: block execution
                        span.set_attribute("validation.schema_bound_blocked", True)
                        span.set_attribute("validation.is_valid", False)
                        span.set_outputs({"error": error_msg, "schema_bound": True})
                        return {
                            "error": error_msg,
                            "error_category": "schema_binding",
                            "ast_validation_result": {"is_valid": False},
                            "result_is_limited": is_limited,
                            "result_limit": limit_value,
                        }
                else:
                    span.set_attribute("validation.schema_bound_blocked", False)
            else:
                span.set_attribute("validation.schema_bound", False)

        # Run AST validation
        result = validate_sql(sql_query)

        # Log validation result
        span.set_attribute("is_valid", str(result.is_valid))
        span.set_attribute("validation.is_valid", result.is_valid)
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

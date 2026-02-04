"""SQL correction node for self-healing queries with MLflow tracing.

Enhanced with error taxonomy for targeted correction strategies.
"""

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

from agent.state import AgentState
from agent.taxonomy.error_taxonomy import classify_error, generate_correction_strategy
from agent.telemetry import telemetry
from agent.telemetry_schema import SpanKind, TelemetryKeys
from common.config.env import get_env_str

load_dotenv()


def correct_sql_node(state: AgentState) -> dict:
    """
    Node: CorrectSQL.

    Analyzes the error using taxonomy classification and generates targeted fixes.
    This implements the self-correction loop with structured error feedback.

    Features:
    - Error classification using taxonomy patterns
    - Targeted correction strategies (not blind regeneration)
    - Consumption of AST validation feedback
    - Correction plan tracking for observability

    Args:
        state: Current agent state with error and current_sql

    Returns:
        dict: Updated state with corrected SQL, error_category, and incremented retry_count
    """
    with telemetry.start_span(
        name="correct_sql",
        span_type=SpanKind.AGENT_NODE,
    ) as span:
        span.set_attribute(TelemetryKeys.EVENT_TYPE, SpanKind.AGENT_NODE)
        span.set_attribute(TelemetryKeys.EVENT_NAME, "correct_sql")
        error = state.get("error")
        current_sql = state.get("current_sql")
        schema_context = state.get("schema_context", "")
        retry_count = state.get("retry_count", 0)
        procedural_plan = state.get("procedural_plan", "")
        ast_validation_result = state.get("ast_validation_result")

        span.set_inputs(
            {
                "error": error,
                "current_sql": current_sql,
                "retry_count": retry_count,
            }
        )

        retry = retry_count + 1

        # Classify the error using taxonomy
        error_category, category_info = classify_error(error or "")
        span.set_attribute("error_category", error_category)
        max_attempts = 3
        span.set_attribute("retry.attempt", retry)
        span.set_attribute("retry.max_attempts", max_attempts)
        span.set_attribute("retry.reason_category", error_category)

        if telemetry.get_current_span():
            telemetry.get_current_span().add_event(
                "agent.retry",
                {
                    "stage": "correct_sql",
                    "reason_category": error_category,
                    "attempt": retry,
                    "max_attempts": max_attempts,
                    "provider": get_env_str("QUERY_TARGET_BACKEND", "postgres"),
                },
            )

        # Generate targeted correction strategy
        correction_strategy = generate_correction_strategy(
            error_message=error or "",
            failed_sql=current_sql or "",
            schema_context=schema_context,
        )

        # Prepare context variables for the prompt
        taxonomy_context = f"""
Additional Context:
- Error Category: {category_info.name}
- Correction Strategy: {category_info.strategy}
"""

        plan_context = ""
        if procedural_plan:
            plan_context = f"""
Original Query Plan:
{procedural_plan}

Ensure your correction follows the original plan's intent.
"""

        ast_context = ""
        if ast_validation_result and not ast_validation_result.get("is_valid"):
            violations = ast_validation_result.get("violations", [])
            if violations:
                violation_details = "\n".join(
                    f"- [{v.get('violation_type')}] {v.get('message')}" for v in violations
                )
                ast_context = f"""
AST Validation Violations:
{violation_details}

Address these specific violations in your correction.
"""

        # Define system template with named placeholders
        system_template = """You are a PostgreSQL expert specializing in error correction.

{correction_strategy}

{taxonomy_context}
{plan_context}
{ast_context}
"""

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_template),
                (
                    "user",
                    """Schema Context:
{schema_context}

Failed Query:
{bad_query}

Error Message:
{error_msg}

Return ONLY the corrected SQL query. No markdown, no explanations.""",
                ),
            ]
        )

        from agent.llm_client import get_llm

        chain = prompt | get_llm(temperature=0)

        response = chain.invoke(
            {
                "correction_strategy": correction_strategy,
                "taxonomy_context": taxonomy_context,
                "plan_context": plan_context,
                "ast_context": ast_context,
                "schema_context": schema_context,
                "bad_query": current_sql,
                "error_msg": error,
            }
        )

        # Capture token usage
        from agent.llm_client import extract_token_usage

        usage_stats = extract_token_usage(response)
        if usage_stats:
            span.set_attributes(usage_stats)

        # Extract SQL from response (remove markdown code blocks if present)
        corrected_sql = response.content.strip()
        if corrected_sql.startswith("```sql"):
            corrected_sql = corrected_sql[6:]
        if corrected_sql.startswith("```"):
            corrected_sql = corrected_sql[3:]
        if corrected_sql.endswith("```"):
            corrected_sql = corrected_sql[:-3]
        corrected_sql = corrected_sql.strip()

        span.set_outputs(
            {
                "corrected_sql": corrected_sql,
                "retry_count": retry,
                "error_category": error_category,
            }
        )

        return {
            "current_sql": corrected_sql,
            "retry_count": retry,
            "error": None,  # Reset error for next attempt
            "error_category": error_category,
            "correction_plan": correction_strategy,
        }

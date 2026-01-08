"""SQL correction node for self-healing queries with MLflow tracing.

Enhanced with error taxonomy for targeted correction strategies.
"""

import mlflow
from agent_core.llm_client import get_llm_client
from agent_core.state import AgentState
from agent_core.taxonomy.error_taxonomy import classify_error, generate_correction_strategy
from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

# Initialize LLM using the factory (temperature=0 for deterministic SQL correction)
llm = get_llm_client(temperature=0)


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
    with mlflow.start_span(
        name="correct_sql",
        span_type=mlflow.entities.SpanType.CHAIN,
    ) as span:
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

        # Generate targeted correction strategy
        correction_strategy = generate_correction_strategy(
            error_message=error or "",
            failed_sql=current_sql or "",
            schema_context=schema_context,
        )

        # Build enhanced system prompt with taxonomy guidance
        system_prompt = f"""You are a PostgreSQL expert specializing in error correction.

{correction_strategy}

Additional Context:
- Error Category: {category_info.name}
- Correction Strategy: {category_info.strategy}
"""

        # Include procedural plan if available (for context preservation)
        if procedural_plan:
            system_prompt += f"""
Original Query Plan:
{procedural_plan}

Ensure your correction follows the original plan's intent.
"""

        # Include AST validation feedback if available
        if ast_validation_result and not ast_validation_result.get("is_valid"):
            violations = ast_validation_result.get("violations", [])
            if violations:
                violation_details = "\n".join(
                    f"- [{v.get('violation_type')}] {v.get('message')}" for v in violations
                )
                system_prompt += f"""
AST Validation Violations:
{violation_details}

Address these specific violations in your correction.
"""

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
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

        chain = prompt | llm

        response = chain.invoke(
            {
                "schema_context": schema_context,
                "bad_query": current_sql,
                "error_msg": error,
            }
        )

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

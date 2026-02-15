"""Error taxonomy for SQL failure mode classification and targeted correction.

This module categorizes common SQL failure modes and provides correction strategies
instead of relying on blind regeneration.
"""

import re
from dataclasses import dataclass
from typing import Optional

from common.models.error_metadata import ErrorCategory


@dataclass
class ErrorTaxonomyEntry:
    """Represents a category of SQL error with correction strategy."""

    name: str
    patterns: list[str]  # Regex patterns to match error messages
    strategy: str  # Correction strategy description
    example_fix: Optional[str] = None  # Example of how to fix


# Comprehensive error taxonomy based on common SQL failure modes, mapped to canonical categories
ERROR_TAXONOMY = {
    ErrorCategory.INVALID_REQUEST: ErrorTaxonomyEntry(
        name="Invalid Request",
        patterns=[
            # Aggregation
            r"must appear in the GROUP BY clause",
            r"aggregate functions are not allowed",
            r"column.*must appear in the GROUP BY",
            r"not a single-group group function",
            # Missing Join
            r"missing FROM-clause entry",
            r"invalid reference to FROM-clause",
            r"relation.*does not exist",
            r"unknown column.*in.*clause",
            r"no such column",
            # Type Mismatch
            r"operator does not exist",
            r"cannot compare",
            r"type mismatch",
            r"invalid input syntax for type",
            r"cannot cast",
            # Ambiguous
            r"ambiguous column",
            r"column reference.*is ambiguous",
            r"ambiguously refers to",
            # Function
            r"function.*does not exist",
            r"function.*with argument types",
            r"unknown function",
            r"no function matches",
            # Nulls
            r"null value in column.*violates",
            r"cannot insert null",
            r"division by zero",
            r"null value not allowed",
            # Subquery
            r"subquery must return only one column",
            r"more than one row returned",
            r"subquery returns more than one row",
            r"scalar subquery",
            # Date/Time
            r"invalid input syntax for type date",
            r"invalid input syntax for type timestamp",
            r"date/time field value out of range",
            r"cannot subtract",
            # Constraints
            r"duplicate key value violates",
            r"violates check constraint",
            r"violates foreign key constraint",
            r"unique constraint",
        ],
        strategy=(
            "Analyze the specific error message to identify the logical flaw. "
            "Common issues include: missing GROUP BY columns, missing JOINs for referenced tables, "
            "type mismatches (requires CAST), ambiguous column names (requires table alias), "
            "or logic errors in subqueries/aggregates."
        ),
        example_fix="Add missing JOINs, fix GROUP BY columns, or CAST types as needed.",
    ),
    ErrorCategory.SYNTAX: ErrorTaxonomyEntry(
        name="SQL Syntax Error",
        patterns=[
            r"syntax error at or near",
            r"syntax error at end of input",
            r"unexpected token",
            r"parse error",
        ],
        strategy=(
            "Check for missing keywords (SELECT, FROM, WHERE), unmatched parentheses, "
            "missing commas, or incorrect clause ordering. Ensure the SQL dialect is valid "
            "PostgreSQL."
        ),
        example_fix="Verify SQL structure: SELECT columns FROM table WHERE condition",
    ),
    ErrorCategory.UNAUTHORIZED: ErrorTaxonomyEntry(
        name="Permission Denied",
        patterns=[
            r"permission denied",
            r"access denied",
            r"insufficient privileges",
        ],
        strategy=(
            "The query accesses restricted data. Use only tables and columns "
            "that the current user has READ access to."
        ),
        example_fix="Remove access to restricted tables or columns",
    ),
    ErrorCategory.RESOURCE_EXHAUSTED: ErrorTaxonomyEntry(
        name="Resource Exhausted",
        patterns=[
            r"result set too large",
            r"exceeded.*rows",
            r"memory exceeded",
        ],
        strategy=(
            "The query matches too many rows or consumes too much memory. "
            "Add or reduce LIMIT clause, use pagination, or add stronger WHERE filtering."
        ),
        example_fix="Add LIMIT 1000 or refine WHERE conditions",
    ),
    ErrorCategory.TIMEOUT: ErrorTaxonomyEntry(
        name="Timeout",
        patterns=[
            r"statement timeout",
            r"query cancelled on user request",
            r"deadline exceeded",
        ],
        strategy=(
            "The query took too long to execute. Optimize for performance: "
            "avoid expensive JOINs on unindexed columns, avoid leading wildcards in LIKE, "
            "and reduce result size."
        ),
        example_fix="Optimize JOINs or filter earlier",
    ),
    ErrorCategory.SCHEMA_DRIFT: ErrorTaxonomyEntry(
        name="Schema Drift",
        patterns=[
            r"schema drift detected",
        ],
        strategy=(
            "The schema has changed since the query was generated. "
            "Regenerate the query using the updated schema context."
        ),
    ),
}


def classify_error(error_message: str) -> tuple[str, ErrorTaxonomyEntry]:
    """
    Classify an error message into a canonical category from the taxonomy.

    Args:
        error_message: The database error message string

    Returns:
        Tuple of (category_key, ErrorTaxonomyEntry) or ("unknown", generic_category)
    """
    error_lower = error_message.lower()

    for category_key, category in ERROR_TAXONOMY.items():
        for pattern in category.patterns:
            if re.search(pattern, error_lower, re.IGNORECASE):
                # Return the canonical ErrorCategory value (string)
                return category_key.value, category

    # Return unknown category
    return ErrorCategory.UNKNOWN.value, ErrorTaxonomyEntry(
        name="Unknown Error",
        patterns=[],
        strategy=(
            "Analyze the error message carefully. Check SQL syntax, "
            "table/column names, and data types. Verify against schema."
        ),
    )


def generate_correction_strategy(
    error_message: str,
    failed_sql: str,
    schema_context: str = "",
    missing_identifiers: Optional[list[str]] = None,
    error_metadata: Optional[dict] = None,
) -> str:
    """
    Generate a detailed correction strategy based on error classification.

    Args:
        error_message: The database error message
        failed_sql: The SQL query that failed
        schema_context: Available schema context
        missing_identifiers: Structured list of identifiers (tables/columns) confirmed missing
        error_metadata: Structured error metadata from the provider

    Returns:
        Formatted correction strategy for the LLM
    """
    category_key, category = classify_error(error_message)

    strategy = f"""## Error Classification: {category.name}

### Error Message
{error_message}
"""

    if error_metadata:
        sql_state = error_metadata.get("sql_state")
        hint = error_metadata.get("hint")
        if sql_state or hint:
            strategy += "\n### Technical Details\n"
            if sql_state:
                strategy += f"- SQLSTATE: {sql_state}\n"
            if hint:
                strategy += f"- Provider Hint: {hint}\n"

    if missing_identifiers:
        strategy += f"""
### Missing Identifiers (Confirmed)
The following identifiers were referenced in the SQL but are NOT present in the schema:
{", ".join(f"'{i}'" for i in missing_identifiers)}
"""

    strategy += f"""
### Failed SQL
{failed_sql}

### Correction Strategy
{category.strategy}
"""

    if category.example_fix:
        strategy += f"""
### Example Fix
{category.example_fix}
"""

    strategy += """
### Instructions
1. Identify the specific line/clause causing the error
2. Apply the correction strategy above
3. Verify the fix against the schema context
4. Return ONLY the corrected SQL query
"""

    return strategy

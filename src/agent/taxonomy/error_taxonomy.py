"""Error taxonomy for SQL failure mode classification and targeted correction.

This module categorizes common SQL failure modes and provides correction strategies
instead of relying on blind regeneration.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ErrorTaxonomyEntry:
    """Represents a category of SQL error with correction strategy."""

    name: str
    patterns: list[str]  # Regex patterns to match error messages
    strategy: str  # Correction strategy description
    example_fix: Optional[str] = None  # Example of how to fix


# Comprehensive error taxonomy based on common SQL failure modes
ERROR_TAXONOMY = {
    "AGGREGATION_MISUSE": ErrorTaxonomyEntry(
        name="Aggregation Misuse",
        patterns=[
            r"must appear in the GROUP BY clause",
            r"aggregate functions are not allowed",
            r"column.*must appear in the GROUP BY",
            r"not a single-group group function",
        ],
        strategy=(
            "Add missing columns to GROUP BY clause, or wrap columns in aggregate functions. "
            "Every non-aggregated column in SELECT must appear in GROUP BY."
        ),
        example_fix="Add 'column_name' to GROUP BY or use MAX(column_name) in SELECT",
    ),
    "MISSING_JOIN": ErrorTaxonomyEntry(
        name="Missing Join",
        patterns=[
            r"missing FROM-clause entry",
            r"invalid reference to FROM-clause",
            r"relation.*does not exist",
            r"unknown column.*in.*clause",
            r"no such column",
        ],
        strategy=(
            "The query references a table or column not included in FROM clause. "
            "Add the required JOIN to connect the missing table."
        ),
        example_fix="Add JOIN table_name ON table_name.id = existing_table.foreign_key",
    ),
    "TYPE_MISMATCH": ErrorTaxonomyEntry(
        name="Type Mismatch",
        patterns=[
            r"operator does not exist",
            r"cannot compare",
            r"type mismatch",
            r"invalid input syntax for type",
            r"cannot cast",
        ],
        strategy=(
            "Cast one or both values to compatible types using ::type or CAST(). "
            "Common casts: ::text, ::integer, ::date, ::timestamp."
        ),
        example_fix="Change column = value to column = value::appropriate_type",
    ),
    "AMBIGUOUS_COLUMN": ErrorTaxonomyEntry(
        name="Ambiguous Column Reference",
        patterns=[
            r"ambiguous column",
            r"column reference.*is ambiguous",
            r"ambiguously refers to",
        ],
        strategy=(
            "Prefix the column with its table name or alias to disambiguate. "
            "Use table_name.column_name or alias.column_name."
        ),
        example_fix="Change 'id' to 'table_name.id' or 't.id'",
    ),
    "SYNTAX_ERROR": ErrorTaxonomyEntry(
        name="SQL Syntax Error",
        patterns=[
            r"syntax error at or near",
            r"syntax error at end of input",
            r"unexpected token",
            r"parse error",
        ],
        strategy=(
            "Check for missing keywords (SELECT, FROM, WHERE), unmatched parentheses, "
            "missing commas, or incorrect clause ordering."
        ),
        example_fix="Verify SQL structure: SELECT columns FROM table WHERE condition",
    ),
    "NULL_HANDLING": ErrorTaxonomyEntry(
        name="NULL Handling Error",
        patterns=[
            r"null value in column.*violates",
            r"cannot insert null",
            r"division by zero",
            r"null value not allowed",
        ],
        strategy=(
            "Use COALESCE() or NULLIF() to handle NULL values. "
            "For division, use NULLIF(divisor, 0) to avoid divide-by-zero."
        ),
        example_fix="Change a/b to a/NULLIF(b, 0) or use COALESCE(column, default)",
    ),
    "SUBQUERY_ERROR": ErrorTaxonomyEntry(
        name="Subquery Error",
        patterns=[
            r"subquery must return only one column",
            r"more than one row returned",
            r"subquery returns more than one row",
            r"scalar subquery",
        ],
        strategy=(
            "Ensure scalar subqueries return exactly one value. "
            "Use LIMIT 1, aggregate functions, or switch to IN/EXISTS for multi-row results."
        ),
        example_fix="Add LIMIT 1 or use IN (subquery) instead of = (subquery)",
    ),
    "PERMISSION_DENIED": ErrorTaxonomyEntry(
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
    "FUNCTION_ERROR": ErrorTaxonomyEntry(
        name="Function Error",
        patterns=[
            r"function.*does not exist",
            r"function.*with argument types",
            r"unknown function",
            r"no function matches",
        ],
        strategy=(
            "Check function name spelling and argument types. "
            "PostgreSQL is case-sensitive for quoted identifiers."
        ),
        example_fix="Verify function exists: Use pg-specific functions like date_trunc()",
    ),
    "CONSTRAINT_VIOLATION": ErrorTaxonomyEntry(
        name="Constraint Violation",
        patterns=[
            r"duplicate key value violates",
            r"violates check constraint",
            r"violates foreign key constraint",
            r"unique constraint",
        ],
        strategy=(
            "For SELECT queries, this shouldn't occur. If it does, check for "
            "INSERT/UPDATE operations that should not be present in read-only mode."
        ),
        example_fix="Ensure query is read-only SELECT statement",
    ),
    "LIMIT_EXCEEDED": ErrorTaxonomyEntry(
        name="Result Limit Exceeded",
        patterns=[
            r"result set too large",
            r"exceeded.*rows",
            r"memory exceeded",
        ],
        strategy=(
            "Add or reduce LIMIT clause. Use pagination with OFFSET, "
            "or add filtering conditions to reduce result size."
        ),
        example_fix="Add LIMIT 1000 or refine WHERE conditions",
    ),
    "DATE_TIME_ERROR": ErrorTaxonomyEntry(
        name="Date/Time Error",
        patterns=[
            r"invalid input syntax for type date",
            r"invalid input syntax for type timestamp",
            r"date/time field value out of range",
            r"cannot subtract",
        ],
        strategy=(
            "Ensure date formats match PostgreSQL expectations (YYYY-MM-DD). "
            "Use proper date functions: date_trunc(), extract(), age()."
        ),
        example_fix="Use '2024-01-01' format or CURRENT_DATE for dates",
    ),
}


def classify_error(error_message: str) -> tuple[str, ErrorTaxonomyEntry]:
    """
    Classify an error message into a category from the taxonomy.

    Args:
        error_message: The database error message string

    Returns:
        Tuple of (category_key, ErrorTaxonomyEntry) or ("UNKNOWN", generic_category)
    """
    error_lower = error_message.lower()

    for category_key, category in ERROR_TAXONOMY.items():
        for pattern in category.patterns:
            if re.search(pattern, error_lower, re.IGNORECASE):
                return category_key, category

    # Return unknown category
    return "UNKNOWN", ErrorTaxonomyEntry(
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

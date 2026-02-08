from typing import List

import sqlglot
from sqlglot import exp

_JSONB_FUNCTIONS = {
    "JSONB_SET",
    "JSONB_EXTRACT_PATH_TEXT",
    "JSONB_EXTRACT_PATH",
    "TO_JSONB",
    "JSONB_BUILD_OBJECT",
}


def validate_redshift_query(sql: str) -> List[str]:
    """Return a list of Redshift incompatibility errors for a SQL string."""
    errors = []

    try:
        # Redshift is based on Postgres, so we parse as Postgres
        expression = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        # If we can't parse it, we can't validate AST.
        # We assume _validate_sql_ast (called earlier) would have caught syntax errors
        # or we let the database handle it.
        # But to be safe, if we can't parse, we might return nothing and let execution fail,
        # or pattern match?
        # Given _validate_sql_ast runs first, this *should* handle valid SQL.
        return []

    for node in expression.walk():
        # Check for ARRAY[...]
        if isinstance(node, exp.Array):
            errors.append(
                "This query uses ARRAY syntax or array operators, "
                "which are not supported on Redshift."
            )
            continue

        # Check for JSONB features
        # 1. Cast to JSONB (::jsonb)
        if isinstance(node, exp.Cast) and node.to.this == exp.DataType.Type.JSONB:
            errors.append("This query uses JSONB, which is not supported on Redshift.")
            continue

        # 2. JSON extraction operators (->, ->>)
        if isinstance(node, (exp.JSONExtract, exp.JSONExtractScalar)):
            errors.append(
                "This query uses JSON extraction operators (->, ->>), "
                "which are not supported on Redshift (use JSON_EXTRACT_PATH_TEXT instead)."
            )
            continue

        # 3. JSONB functions
        func_name = ""
        # Check Anonymous first because it likely inherits from Func
        if isinstance(node, exp.Anonymous):
            if isinstance(node.this, str):
                func_name = node.this.upper()
            elif hasattr(node.this, "name"):
                func_name = node.this.name.upper()
            else:
                func_name = str(node.this).upper()
        elif isinstance(node, exp.Func):
            func_name = node.sql_name().upper()

        if func_name in _JSONB_FUNCTIONS:
            errors.append(f"This query uses {func_name}, which is not supported on Redshift.")

    # Deduplicate errors
    return list(set(errors))

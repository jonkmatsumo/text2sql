import re
from typing import Dict, List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_snowflake(
    sql: str, params: List[object]
) -> Tuple[str, Dict[str, object]]:
    """Translate Postgres-style $N placeholders into Snowflake pyformat binds."""
    placeholders = PLACEHOLDER_PATTERN.findall(sql)

    if not placeholders:
        if params:
            raise ValueError("Snowflake query received params but no $N placeholders were found.")
        return sql, {}

    indices = [int(value) for value in placeholders]
    if any(index == 0 for index in indices):
        raise ValueError("Invalid placeholder index $0 in query.")

    max_index = max(indices)
    expected = set(range(1, max_index + 1))
    actual = set(indices)
    if actual != expected:
        raise ValueError(
            "Snowflake placeholders must be sequential without gaps (e.g., $1, $2, ...)."
        )
    if len(params) < max_index:
        raise ValueError(
            "Not enough parameters supplied for Snowflake query: expected "
            f"{max_index}, got {len(params)}."
        )

    def _replace(match: re.Match) -> str:
        index = match.group(1)
        return f"%(p{index})s"

    translated_sql = PLACEHOLDER_PATTERN.sub(_replace, sql)
    bound_params = {f"p{idx}": params[idx - 1] for idx in range(1, max_index + 1)}
    return translated_sql, bound_params

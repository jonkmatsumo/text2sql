import re
from typing import List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_athena(sql: str, params: List[object]) -> Tuple[str, List[str]]:
    """Translate Postgres-style $N placeholders into Athena positional params."""
    placeholders = PLACEHOLDER_PATTERN.findall(sql)

    if not placeholders:
        if params:
            raise ValueError("Athena query received params but no $N placeholders were found.")
        return sql, []

    indices = [int(value) for value in placeholders]
    if any(index == 0 for index in indices):
        raise ValueError("Invalid placeholder index $0 in query.")

    max_index = max(indices)
    expected = set(range(1, max_index + 1))
    actual = set(indices)
    if actual != expected:
        raise ValueError("Athena placeholders must be sequential without gaps (e.g., $1, $2, ...).")
    if len(params) < max_index:
        raise ValueError(
            "Not enough parameters supplied for Athena query: expected "
            f"{max_index}, got {len(params)}."
        )

    def _replace(match: re.Match) -> str:
        return "?"

    translated_sql = PLACEHOLDER_PATTERN.sub(_replace, sql)
    ordered_params = []
    for index_str in placeholders:
        value = params[int(index_str) - 1]
        ordered_params.append(str(value) if value is not None else "")
    return translated_sql, ordered_params

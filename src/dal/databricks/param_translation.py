import re
from typing import List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_databricks(
    sql: str, params: List[object]
) -> Tuple[str, List[dict]]:
    """Translate Postgres-style $N placeholders into Databricks named params."""
    placeholders = PLACEHOLDER_PATTERN.findall(sql)

    if not placeholders:
        if params:
            raise ValueError("Databricks query received params but no $N placeholders were found.")
        return sql, []

    indices = [int(value) for value in placeholders]
    if any(index == 0 for index in indices):
        raise ValueError("Invalid placeholder index $0 in query.")

    max_index = max(indices)
    expected = set(range(1, max_index + 1))
    actual = set(indices)
    if actual != expected:
        raise ValueError(
            "Databricks placeholders must be sequential without gaps (e.g., $1, $2, ...)."
        )
    if len(params) < max_index:
        raise ValueError(
            "Not enough parameters supplied for Databricks query: expected "
            f"{max_index}, got {len(params)}."
        )

    def _replace(match: re.Match) -> str:
        index = match.group(1)
        return f":p{index}"

    translated_sql = PLACEHOLDER_PATTERN.sub(_replace, sql)
    ordered_params = []
    for index_str in placeholders:
        value = params[int(index_str) - 1]
        ordered_params.append(_build_param(f"p{index_str}", value))
    return translated_sql, ordered_params


def _build_param(name: str, value: object) -> dict:
    return {"name": name, "value": value, "type": _infer_type(value)}


def _infer_type(value: object) -> str:
    if value is None:
        return "STRING"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "DOUBLE"
    return "STRING"

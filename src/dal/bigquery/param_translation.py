import re
from typing import List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_bigquery(
    sql: str, params: List[object]
) -> Tuple[str, List[object]]:
    """Translate Postgres-style $N placeholders into BigQuery named params."""
    placeholders = PLACEHOLDER_PATTERN.findall(sql)

    if not placeholders:
        if params:
            raise ValueError("BigQuery query received params but no $N placeholders were found.")
        return sql, []

    indices = [int(value) for value in placeholders]
    if any(index == 0 for index in indices):
        raise ValueError("Invalid placeholder index $0 in query.")

    max_index = max(indices)
    expected = set(range(1, max_index + 1))
    actual = set(indices)
    if actual != expected:
        raise ValueError(
            "BigQuery placeholders must be sequential without gaps (e.g., $1, $2, ...)."
        )
    if len(params) < max_index:
        raise ValueError(
            "Not enough parameters supplied for BigQuery query: expected "
            f"{max_index}, got {len(params)}."
        )

    def _replace(match: re.Match) -> str:
        index = match.group(1)
        return f"@p{index}"

    translated_sql = PLACEHOLDER_PATTERN.sub(_replace, sql)
    query_params = _build_query_params(params[:max_index])
    return translated_sql, query_params


def _build_query_params(values: List[object]) -> List[object]:
    from google.cloud import bigquery

    query_params = []
    for idx, value in enumerate(values, start=1):
        param_type = _infer_type(value)
        query_params.append(bigquery.ScalarQueryParameter(f"p{idx}", param_type, value))
    return query_params


def _infer_type(value: object) -> str:
    if value is None:
        return "STRING"
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    return "STRING"

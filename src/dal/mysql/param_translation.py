import re
from typing import Any, List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_mysql(sql: str, params: List[Any]) -> Tuple[str, List[Any]]:
    """Translate Postgres-style $N placeholders to MySQL %s placeholders."""
    matches = list(PLACEHOLDER_PATTERN.finditer(sql))
    if not matches:
        if params:
            raise ValueError("MySQL query received params but no $N placeholders were found.")
        return sql, []

    indices = []
    for match in matches:
        idx = int(match.group(1))
        if idx <= 0:
            raise ValueError(f"Invalid placeholder index ${idx}; placeholders must start at $1.")
        indices.append(idx)

    max_index = max(indices)
    expected = set(range(1, max_index + 1))
    if set(indices) != expected:
        indices_found = sorted(set(indices))
        raise ValueError(
            f"Invalid placeholder sequence: expected $1..${max_index} without gaps, got "
            f"{indices_found}."
        )
    if max_index > len(params):
        raise ValueError(
            f"Not enough parameters for placeholders: expected {max_index}, got {len(params)}."
        )

    mysql_params = [params[i - 1] for i in indices]
    mysql_sql = PLACEHOLDER_PATTERN.sub("%s", sql)
    return mysql_sql, mysql_params

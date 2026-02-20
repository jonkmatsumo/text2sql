import re
from typing import Any, List, Tuple

PLACEHOLDER_PATTERN = re.compile(r"\$(\d+)")


def translate_postgres_params_to_sqlite(sql: str, params: List[Any]) -> Tuple[str, List[Any]]:
    """Translate Postgres-style $N placeholders to SQLite ? placeholders."""
    matches = list(PLACEHOLDER_PATTERN.finditer(sql))
    qmark_count = sql.count("?")

    if not matches:
        if qmark_count:
            if len(params) < qmark_count:
                raise ValueError(
                    "Not enough parameters for ? placeholders: "
                    f"expected {qmark_count}, got {len(params)}."
                )
            if len(params) > qmark_count:
                raise ValueError(
                    "Too many parameters for ? placeholders: "
                    f"expected {qmark_count}, got {len(params)}."
                )
            return sql, list(params)
        if params:
            raise ValueError("SQLite query received params but no $N placeholders were found.")
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

    remaining = params[max_index:]
    if len(remaining) < qmark_count:
        raise ValueError(
            "Not enough parameters for ? placeholders: "
            f"expected {qmark_count}, got {len(remaining)}."
        )
    if len(remaining) > qmark_count:
        raise ValueError(
            "Too many parameters for ? placeholders: "
            f"expected {qmark_count}, got {len(remaining)}."
        )

    sqlite_params = [params[i - 1] for i in indices]
    sqlite_params.extend(remaining)
    sqlite_sql = PLACEHOLDER_PATTERN.sub("?", sql)
    return sqlite_sql, sqlite_params

"""Load queries from JSON files for seeding database."""

import json
from glob import glob
from pathlib import Path
from typing import Optional


def load_queries_from_files(
    patterns: list[str],
    base_path: Optional[Path] = None,
    required_fields: Optional[list[str]] = None,
) -> list[dict]:
    """Load queries from multiple JSON files matching patterns.

    Args:
        patterns: List of file paths or glob patterns (e.g., ["seed.json", "queries/*.json"])
        base_path: Base directory to resolve relative paths. Defaults to cwd.
        required_fields: List of fields each query must have. Raises ValueError if missing.

    Returns:
        List of query dictionaries merged from all matching files.

    Raises:
        FileNotFoundError: If no files match any pattern.
        ValueError: If required fields are missing from a query.
        json.JSONDecodeError: If a file contains invalid JSON.
    """
    if base_path is None:
        base_path = Path.cwd()
    else:
        base_path = Path(base_path)

    all_queries: list[dict] = []
    files_found: set[Path] = set()

    for pattern in patterns:
        # Handle both absolute and relative paths
        if Path(pattern).is_absolute():
            matches = glob(pattern)
        else:
            matches = glob(str(base_path / pattern))

        for match in matches:
            file_path = Path(match)
            if file_path.is_file():
                files_found.add(file_path)

    if not files_found:
        raise FileNotFoundError(f"No files found matching patterns: {patterns} in {base_path}")

    for file_path in sorted(files_found):
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)

        # Support both flat list and nested {"queries": [...]} format
        if isinstance(data, list):
            queries = data
        elif isinstance(data, dict) and "queries" in data:
            queries = data["queries"]
        else:
            raise ValueError(
                f"Invalid format in {file_path}: expected list or dict with 'queries' key"
            )

        # Validate required fields
        if required_fields:
            for i, query in enumerate(queries):
                missing = [f for f in required_fields if f not in query]
                if missing:
                    raise ValueError(f"Query {i} in {file_path} missing required fields: {missing}")

        all_queries.extend(queries)

    return all_queries


def load_examples_for_vector_db(
    patterns: list[str],
    base_path: Optional[Path] = None,
) -> list[dict]:
    """Load queries formatted for vector DB seeding (question + query only).

    Args:
        patterns: List of file paths or glob patterns.
        base_path: Base directory to resolve relative paths.

    Returns:
        List of dicts with 'question' and 'query' keys.
    """
    queries = load_queries_from_files(
        patterns=patterns,
        base_path=base_path,
        required_fields=["question", "query"],
    )

    return [{"question": q["question"], "query": q["query"]} for q in queries]


def load_golden_dataset(
    patterns: list[str],
    base_path: Optional[Path] = None,
    tenant_id: int = 1,
) -> list[dict]:
    """Load queries formatted for golden dataset (full metadata).

    Args:
        patterns: List of file paths or glob patterns.
        base_path: Base directory to resolve relative paths.
        tenant_id: Default tenant_id if not specified in query.

    Returns:
        List of dicts with full test case data.
    """
    queries = load_queries_from_files(
        patterns=patterns,
        base_path=base_path,
        required_fields=["question", "query"],
    )

    return [
        {
            "question": q["question"],
            "ground_truth_sql": q["query"],
            "expected_result": q.get("expected_result"),
            "expected_row_count": q.get("expected_row_count"),
            "category": q.get("category", "general"),
            "difficulty": q.get("difficulty", "medium"),
            "tenant_id": q.get("tenant_id", tenant_id),
        }
        for q in queries
    ]

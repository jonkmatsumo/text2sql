"""JSON Loader for seeding data."""

import json
from pathlib import Path
from typing import Any, Dict, List


def load_from_directory(directory: Path) -> List[Dict[str, Any]]:
    """Load all JSON files from a directory and extract queries.

    Supports both:
    1. List of objects: [ {..}, {..} ]
    2. Dict with 'queries' key: { "queries": [ {..} ] }

    Args:
        directory: Path to directory containing .json files.

    Returns:
        List of query dictionaries.
    """
    if not directory.exists():
        print(f"Warning: Seed directory not found: {directory}")
        return []

    data = []
    # Use rglob to recursively find .json files in subdirectories
    for file_path in directory.rglob("*.json"):
        if file_path.name == "tables.json":
            continue

        try:
            with open(file_path, "r") as f:
                content = json.load(f)

            if isinstance(content, list):
                data.extend(content)
            elif isinstance(content, dict) and "queries" in content:
                data.extend(content["queries"])
            else:
                print(f"Skipping {file_path}: Invalid format")

        except Exception as e:
            print(f"Error loading {file_path}: {e}")

    print(f"Loaded {len(data)} items from {directory}")
    return data


def load_table_summaries(base_path: Path) -> List[Dict[str, Any]]:
    """Load table summaries from tables.json."""
    path = base_path / "tables.json"
    if not path.exists():
        print(f"Warning: {path} not found.")
        return []

    try:
        with open(path, "r") as f:
            data = json.load(f)
            if not isinstance(data, list):
                print(f"Error: {path} must contain a list of objects.")
                return []
            print(f"Loaded {len(data)} table summaries from parameters.")
            return data
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return []

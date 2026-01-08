#!/usr/bin/env python3
"""
Prune Seeds Script.

Removes any seed query that has an empty 'expected_result' or 'expected_row_count' of 0.
Reports the count of queries before and after pruning for each file.
"""

import json
import sys
from pathlib import Path


def prune_file(file_path: Path):
    """Prune queries from a seed file based on results."""
    with open(file_path, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"❌ Error decoding {file_path.name}")
            return None

    # Handle unified schema vs legacy
    if isinstance(data, dict) and "queries" in data:
        queries = data["queries"]
        is_wrapped = True
    elif isinstance(data, list):
        queries = data
        is_wrapped = False
    else:
        print(f"⚠️  Unknown format for {file_path.name}, skipping.")
        return None

    original_count = len(queries)

    # Filter valid queries: expected_row_count > 0 OR expected_result is not empty
    valid_queries = []
    for q in queries:
        # Check both fields to be safe
        count = q.get("expected_row_count", 0)
        result = q.get("expected_result", [])

        if count > 0 and result:
            valid_queries.append(q)

    pruned_count = len(valid_queries)

    # Write back if changed
    if pruned_count < original_count:
        if is_wrapped:
            data["queries"] = valid_queries
            output = data
        else:
            output = valid_queries

        with open(file_path, "w") as f:
            json.dump(output, f, indent=2)

    return {
        "file": file_path.name,
        "original": original_count,
        "remaining": pruned_count,
        "removed": original_count - pruned_count,
    }


def main():
    """Execute main pruning process."""
    base_path = Path(__file__).parent.parent.parent / "database" / "queries"
    if not base_path.exists():
        # Try local path if running inside container mapping
        base_path = Path("/app/queries")

    if not base_path.exists():
        print(f"Queries directory not found at {base_path}")
        sys.exit(1)

    all_stats = []

    # Process by_table
    for f in sorted((base_path / "by_table").glob("*.json")):
        stats = prune_file(f)
        if stats:
            stats["type"] = "by_table"
            all_stats.append(stats)

    # Process complex
    for f in sorted((base_path / "complex").glob("*.json")):
        stats = prune_file(f)
        if stats:
            stats["type"] = "complex"
            all_stats.append(stats)

    # Process joins
    for f in sorted((base_path / "joins").glob("*.json")):
        stats = prune_file(f)
        if stats:
            stats["type"] = "joins"
            all_stats.append(stats)

    # Print Report
    print(f"{'File':<30} | {'Type':<10} | {'Before':<8} | {'After':<8} | {'Removed':<8}")
    print("-" * 75)

    total_removed = 0
    total_remaining = 0

    for s in all_stats:
        print(
            f"{s['file']:<30} | {s['type']:<10} | {s['original']:<8} | "
            f"{s['remaining']:<8} | {s['removed']:<8}"
        )
        total_removed += s["removed"]
        total_remaining += s["remaining"]

    print("-" * 75)
    print(f"{'TOTAL':<30} | {'':<10} | {'':<8} | {total_remaining:<8} | {total_removed:<8}")


if __name__ == "__main__":
    main()

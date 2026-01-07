"""Seed golden dataset with test cases for evaluation."""

import argparse
import asyncio
import json
from pathlib import Path

from dotenv import load_dotenv
from mcp_server.db import Database
from mcp_server.seeding.loader import load_golden_dataset

load_dotenv()

# Default JSON files if none specified
DEFAULT_PATTERNS = ["database/seed_queries.json"]


async def seed_golden_dataset_db(
    patterns: list[str],
    base_path: Path,
    tenant_id: int = 1,
    dry_run: bool = False,
) -> int:
    """Seed the database with golden test cases.

    Args:
        patterns: File patterns to load queries from.
        base_path: Base path to resolve relative patterns.
        tenant_id: Default tenant ID for test cases.
        dry_run: If True, only print what would be done.

    Returns:
        Number of test cases seeded.
    """
    test_cases = load_golden_dataset(
        patterns=patterns,
        base_path=base_path,
        tenant_id=tenant_id,
    )

    if not test_cases:
        print("No test cases found to seed.")
        return 0

    if not dry_run:
        await Database.init()

    try:
        print(f"Seeding {len(test_cases)} golden test cases...")

        for i, test_case in enumerate(test_cases, 1):
            print(f"\n[{i}/{len(test_cases)}] {test_case['question']}")
            print(f"  Category: {test_case['category']}, Difficulty: {test_case['difficulty']}")

            if dry_run:
                print("  (dry run - skipping)")
                continue

            # Serialize expected_result to JSON string if present
            expected_result = test_case.get("expected_result")
            if expected_result is not None:
                expected_result = json.dumps(expected_result)

            async with Database.get_connection(test_case["tenant_id"]) as conn:
                await conn.execute(
                    """
                    INSERT INTO golden_dataset (
                        question, ground_truth_sql, expected_result,
                        expected_row_count, category, difficulty, tenant_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT DO NOTHING
                """,
                    test_case["question"],
                    test_case["ground_truth_sql"].strip(),
                    expected_result,
                    test_case.get("expected_row_count"),
                    test_case["category"],
                    test_case["difficulty"],
                    test_case["tenant_id"],
                )

            print("  ✓ Inserted into database")

        print(f"\n✓ Seeded {len(test_cases)} test cases")
        return len(test_cases)

    finally:
        if not dry_run:
            await Database.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Seed golden dataset for evaluation")
    parser.add_argument(
        "--files",
        nargs="+",
        default=DEFAULT_PATTERNS,
        help="JSON file patterns to load (default: database/seed_queries.json)",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=Path.cwd(),
        help="Base path for resolving relative file patterns",
    )
    parser.add_argument(
        "--tenant-id",
        type=int,
        default=1,
        help="Default tenant ID for test cases (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes",
    )

    args = parser.parse_args()
    asyncio.run(seed_golden_dataset_db(args.files, args.base_path, args.tenant_id, args.dry_run))


if __name__ == "__main__":
    main()

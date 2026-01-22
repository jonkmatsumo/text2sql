#!/usr/bin/env python3
"""Replay harness for testing cache constraint validation.

This script loads a test corpus of rating queries and validates that
the constraint extraction correctly identifies rating and limit values.

Usage:
    python scripts/replay_cache_tests.py [--corpus PATH]
"""

import argparse
import json
import sys
from pathlib import Path

# Add agent to path for imports (must be before agent_core import)
sys.path.insert(0, str(Path(__file__).parent.parent / "agent" / "src"))

from agent_core.cache.constraint_extractor import extract_constraints  # noqa: E402


def load_corpus(corpus_path: Path) -> list:
    """Load test corpus from JSON file."""
    with open(corpus_path) as f:
        return json.load(f)


def run_tests(corpus: list, verbose: bool = True) -> dict:
    """Run constraint extraction tests on corpus.

    Returns:
        dict with pass_count, fail_count, and failures list
    """
    results = {"pass_count": 0, "fail_count": 0, "failures": []}

    for i, test_case in enumerate(corpus):
        query = test_case["query"]
        expected_rating = test_case.get("expected_rating")
        expected_limit = test_case.get("expected_limit")

        constraints = extract_constraints(query)

        # Check rating
        rating_match = constraints.rating == expected_rating
        # Check limit (None in test case means we don't care)
        limit_match = expected_limit is None or constraints.limit == expected_limit

        if rating_match and limit_match:
            results["pass_count"] += 1
            if verbose:
                print(f"✓ Test {i+1}: '{query[:50]}...'")
        else:
            results["fail_count"] += 1
            failure = {
                "test_index": i + 1,
                "query": query,
                "expected_rating": expected_rating,
                "got_rating": constraints.rating,
                "expected_limit": expected_limit,
                "got_limit": constraints.limit,
            }
            results["failures"].append(failure)
            if verbose:
                print(f"✗ Test {i+1}: '{query[:50]}...'")
                if not rating_match:
                    print(
                        f"    Rating: expected '{expected_rating}', " f"got '{constraints.rating}'"
                    )
                if not limit_match:
                    print(f"    Limit: expected {expected_limit}, " f"got {constraints.limit}")

    return results


def main():
    """Run the replay harness CLI."""
    parser = argparse.ArgumentParser(description="Replay harness for cache constraint validation")
    parser.add_argument(
        "--corpus",
        type=Path,
        default=Path(__file__).parent.parent
        / "database"
        / "query-target"
        / "corpus"
        / "rating_queries.json",
        help="Path to corpus JSON file",
    )
    parser.add_argument("--quiet", action="store_true", help="Only show summary")

    args = parser.parse_args()

    if not args.corpus.exists():
        # In synthetic mode, it's expected that rating_queries.json might not be relevant or present
        # Check if we should fail or just skip
        from common.config.dataset import get_dataset_mode

        if get_dataset_mode() == "synthetic":
            print(f"Synthetic mode: corpus not found at {args.corpus}. Skipping replay tests.")
            sys.exit(0)

        print(f"Error: Corpus file not found: {args.corpus}")
        sys.exit(1)

    print(f"Loading corpus from: {args.corpus}")
    corpus = load_corpus(args.corpus)
    print(f"Running {len(corpus)} tests...\n")

    results = run_tests(corpus, verbose=not args.quiet)

    # Summary
    print("\n" + "=" * 50)
    print(f"RESULTS: {results['pass_count']} passed, {results['fail_count']} failed")

    if results["failures"]:
        print("\nFailed tests:")
        for f in results["failures"]:
            print(f"  - Test {f['test_index']}: {f['query'][:50]}...")

    # Exit with error if any failures
    sys.exit(1 if results["fail_count"] > 0 else 0)


if __name__ == "__main__":
    main()

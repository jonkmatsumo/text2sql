"""Evaluation runner for Golden Dataset regression testing.

Supports both database-backed and file-based golden datasets.
"""

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg
from dotenv import load_dotenv

# Add agent to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Add database/query-target to path for golden dataset imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "database" / "query-target"))

from agent_core.graph import run_agent_with_tracing  # noqa: E402
from golden import (  # noqa: E402
    GoldenDatasetNotFoundError,
    GoldenDatasetValidationError,
    load_test_cases,
)

from schema.evaluation.metrics import MetricSuiteV1  # noqa: E402

load_dotenv()

logger = logging.getLogger(__name__)


def _get_db_config() -> Dict[str, Any]:
    """Get database configuration from environment."""
    from common.config.dataset import get_default_db_name

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME", get_default_db_name()),
        "user": os.getenv("DB_USER", "text2sql_ro"),
        "password": os.getenv("DB_PASS", "secure_agent_pass"),
    }


async def fetch_test_cases_from_db(
    tenant_id: int = 1, category: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Fetch active test cases from Golden Dataset table in database."""
    config = _get_db_config()
    conn = await asyncpg.connect(**config)

    try:
        query = """
            SELECT test_id, question, ground_truth_sql, expected_row_count,
                   category, difficulty, tenant_id
            FROM golden_dataset
            WHERE is_active = true AND tenant_id = $1
        """
        params = [tenant_id]

        if category:
            query += " AND category = $2"
            params.append(category)

        query += " ORDER BY difficulty, test_id"

        rows = await conn.fetch(query, *params)
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def fetch_test_cases_from_file(
    dataset_mode: str = "synthetic",
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch test cases from file-based golden dataset.

    Convert GoldenTestCase objects to dict format compatible with evaluation.
    """
    test_cases = load_test_cases(
        dataset_mode=dataset_mode,
        category=category,
        difficulty=difficulty,
    )

    return [
        {
            "test_id": tc.id,
            "question": tc.nlq,
            "ground_truth_sql": tc.expected_sql,
            "expected_row_count": tc.expected_row_count,
            "expected_row_count_min": tc.expected_row_count_min,
            "expected_row_count_max": tc.expected_row_count_max,
            "expected_columns": tc.expected_columns,
            "category": tc.category,
            "difficulty": tc.difficulty,
            "intent": tc.intent,
        }
        for tc in test_cases
    ]


async def execute_ground_truth_sql(sql: str, tenant_id: int) -> List[Dict[str, Any]]:
    """Execute ground truth SQL to get expected result."""
    config = _get_db_config()
    conn = await asyncpg.connect(**config)

    try:
        # Set tenant context
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(tenant_id),
        )

        rows = await conn.fetch(sql)
        return [dict(row) for row in rows]
    finally:
        await conn.close()


def validate_result_shape(
    actual_result: List[Dict[str, Any]],
    test_case: Dict[str, Any],
) -> tuple[bool, Optional[str]]:
    """Validate result shape against test case expectations.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if actual_result is None:
        return False, "No result returned"

    actual_row_count = len(actual_result)

    # Check exact row count
    if test_case.get("expected_row_count") is not None:
        expected = test_case["expected_row_count"]
        if actual_row_count != expected:
            return False, f"Row count mismatch: got {actual_row_count}, expected {expected}"

    # Check row count range
    min_rows = test_case.get("expected_row_count_min")
    max_rows = test_case.get("expected_row_count_max")
    if min_rows is not None and actual_row_count < min_rows:
        return False, f"Too few rows: got {actual_row_count}, expected >= {min_rows}"
    if max_rows is not None and actual_row_count > max_rows:
        return False, f"Too many rows: got {actual_row_count}, expected <= {max_rows}"

    # Check expected columns
    expected_columns = test_case.get("expected_columns", [])
    if expected_columns and actual_result:
        actual_columns = list(actual_result[0].keys())
        missing = set(expected_columns) - set(actual_columns)
        if missing:
            return False, f"Missing columns: {missing}"

    return True, None


async def evaluate_test_case(
    test_case: Dict[str, Any],
    tenant_id: int,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Evaluate a single test case.

    Args:
        test_case: Test case dict from DB or file.
        tenant_id: Tenant ID for agent execution.
        dry_run: If True, skip agent execution (for validation only).
    """
    test_id = test_case["test_id"]
    question = test_case["question"]
    ground_truth_sql = test_case.get("ground_truth_sql")

    print(f"\n[Test {test_id}] {question}")

    if dry_run:
        print("  ⏭ SKIPPED (dry run)")
        return {
            "test_id": test_id,
            "is_correct": None,
            "execution_time_ms": 0,
            "skipped": True,
        }

    start_time = time.time()

    try:
        # Run agent
        result = await run_agent_with_tracing(
            question=question,
            tenant_id=tenant_id,
            session_id=f"eval-{test_id}",
        )

        execution_time_ms = int((time.time() - start_time) * 1000)

        generated_sql = result.get("current_sql")
        actual_result = result.get("query_result")
        error = result.get("error")

        # Determine Metrics V1
        metrics = MetricSuiteV1.compute_all(generated_sql, ground_truth_sql or "")
        is_correct = metrics["exact_match"]
        error_message = None

        if error:
            error_message = str(error)
        elif not generated_sql:
            error_message = "No SQL generated"
        elif actual_result is not None:
            # Validate result shape (legacy check)
            shape_valid, shape_error = validate_result_shape(actual_result, test_case)
            if not shape_valid:
                error_message = shape_error

        status = "✓ PASS" if is_correct else "✗ FAIL"
        print(
            f"  {status} - EM: {is_correct}, Structural: {metrics['structural_score']}, "
            f"Rows: {len(actual_result) if actual_result else 0}, "
            f"Time: {execution_time_ms}ms"
        )
        if error_message:
            print(f"  Error: {error_message}")

        return {
            "test_id": test_id,
            "is_correct": is_correct,
            "exact_match": metrics["exact_match"],
            "structural_score": metrics["structural_score"],
            "subscores": metrics["subscores"],
            "generated_tables": metrics["generated_tables"],
            "expected_tables": metrics["expected_tables"],
            "parse_errors": metrics["parse_errors"],
            "execution_time_ms": execution_time_ms,
            "error_message": error_message,
        }

    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        error_message = str(e)
        print(f"  ✗ ERROR - {error_message}")
        return {
            "test_id": test_id,
            "is_correct": False,
            "execution_time_ms": execution_time_ms,
            "error_message": error_message,
        }


async def store_evaluation_result(
    test_id: Any,
    generated_sql: Optional[str],
    actual_result: Optional[List],
    actual_row_count: int,
    is_correct: bool,
    error_message: Optional[str],
    token_count: Optional[int],
    execution_time_ms: int,
    tenant_id: int,
) -> None:
    """Store evaluation result in database."""
    config = _get_db_config()
    conn = await asyncpg.connect(**config)

    try:
        await conn.execute(
            """
            INSERT INTO evaluation_results (
                test_id, generated_sql, actual_result, actual_row_count,
                is_correct, error_message, token_count, execution_time_ms, tenant_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
            str(test_id),  # Convert to string in case it's from file-based tests
            generated_sql,
            json.dumps(actual_result) if actual_result else None,
            actual_row_count,
            is_correct,
            error_message,
            token_count,
            execution_time_ms,
            tenant_id,
        )
    finally:
        await conn.close()


def validate_golden_dataset_cli(
    dataset_mode: str,
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
) -> bool:
    """Validate golden dataset without running agent (for CI).

    Returns True if validation passes.
    """
    print("=" * 60)
    print(f"Validating Golden Dataset (mode: {dataset_mode})")
    print("=" * 60)

    try:
        test_cases = fetch_test_cases_from_file(
            dataset_mode=dataset_mode,
            category=category,
            difficulty=difficulty,
        )
        print(f"\n✓ Loaded {len(test_cases)} test cases")

        # Validate each test case has required fields
        errors = []
        for tc in test_cases:
            if not tc.get("question"):
                errors.append(f"{tc['test_id']}: missing question/nlq")
            if not tc.get("ground_truth_sql"):
                errors.append(f"{tc['test_id']}: missing ground_truth_sql/expected_sql")

        if errors:
            print("\n✗ Validation errors:")
            for e in errors:
                print(f"  - {e}")
            return False

        # Print summary by category
        categories = {}
        for tc in test_cases:
            cat = tc.get("category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1

        print("\n  Categories:")
        for cat, count in sorted(categories.items()):
            print(f"    {cat}: {count}")

        difficulties = {}
        for tc in test_cases:
            diff = tc.get("difficulty", "unknown")
            difficulties[diff] = difficulties.get(diff, 0) + 1

        print("\n  Difficulties:")
        for diff, count in sorted(difficulties.items()):
            print(f"    {diff}: {count}")

        print("\n✓ Golden dataset validation passed")
        return True

    except GoldenDatasetNotFoundError as e:
        print(f"\n✗ Golden dataset not found: {e}")
        return False
    except GoldenDatasetValidationError as e:
        print(f"\n✗ Golden dataset validation failed: {e}")
        return False


async def run_evaluation_suite(
    tenant_id: int = 1,
    category: Optional[str] = None,
    difficulty: Optional[str] = None,
    dataset_mode: Optional[str] = None,
    golden_only: bool = False,
    dry_run: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run full evaluation suite against Golden Dataset.

    Args:
        tenant_id: Tenant ID for multi-tenant isolation.
        category: Filter by category.
        difficulty: Filter by difficulty.
        dataset_mode: Dataset mode (synthetic/pagila). If None, uses DATASET_MODE env var.
        golden_only: If True, load from file-based golden dataset instead of DB.
        dry_run: If True, validate without running agent.
    """
    # Enforce dataset mode from env if not specified
    if dataset_mode is None:
        from common.config.dataset import get_dataset_mode

        dataset_mode = get_dataset_mode()

    print("=" * 60)
    print(f"Golden Dataset Evaluation Suite (mode: {dataset_mode})")
    print("=" * 60)

    # Fetch test cases
    if golden_only:
        print("\nLoading from file-based golden dataset...")
        try:
            test_cases = fetch_test_cases_from_file(
                dataset_mode=dataset_mode,
                category=category,
                difficulty=difficulty,
            )
        except GoldenDatasetNotFoundError as e:
            print(f"\n✗ ERROR: {e}")
            print("Golden dataset file is required for --golden-only mode.")
            sys.exit(1)
        except GoldenDatasetValidationError as e:
            print(f"\n✗ ERROR: {e}")
            sys.exit(1)
    else:
        print("\nLoading from database...")
        test_cases = await fetch_test_cases_from_db(tenant_id=tenant_id, category=category)

    if not test_cases:
        print("No test cases found.")
        return None

    print(f"Found {len(test_cases)} test cases to evaluate")

    if dry_run:
        print("\n[DRY RUN - Agent execution skipped]")

    # Run evaluation
    results = []
    for test_case in test_cases:
        result = await evaluate_test_case(test_case, tenant_id, dry_run=dry_run)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("Evaluation Summary")
    print("=" * 60)

    evaluated = [r for r in results if not r.get("skipped")]
    total = len(evaluated)

    if total == 0:
        print("No tests executed (all skipped or dry run).")
        return {"total": 0, "passed": 0, "failed": 0, "accuracy": 0}

    passed = sum(1 for r in evaluated if r["is_correct"])  # is_correct is exact_match
    failed = total - passed
    avg_time = sum(r["execution_time_ms"] for r in evaluated) / total

    exact_match_rate = passed / total if total > 0 else 0
    structural_scores = [r["structural_score"] for r in evaluated]
    avg_structural_score = sum(structural_scores) / total if total > 0 else 0
    min_structural_score = min(structural_scores) if structural_scores else 0

    print(f"Total Tests: {total}")
    print(f"Exact Match Rate: {exact_match_rate * 100:.1f}% ({passed}/{total})")
    print(f"Avg Structural Score: {avg_structural_score:.3f}")
    print(f"Min Structural Score: {min_structural_score:.3f}")
    print(f"Average Execution Time: {avg_time:.0f}ms")

    summary = {
        "total": total,
        "passed": passed,  # mapped to exact_match_count
        "failed": failed,
        "accuracy": exact_match_rate,  # mapped to exact_match_rate
        "exact_match_rate": exact_match_rate,
        "avg_structural_score": avg_structural_score,
        "min_structural_score": min_structural_score,
        "avg_time_ms": avg_time,
    }

    if golden_only:
        # Emit artifacts
        output_dir = Path("evaluation_artifacts")
        output_dir.mkdir(exist_ok=True)

        with open(output_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        with open(output_dir / "results.json", "w") as f:
            json.dump(results, f, indent=2)

        with open(output_dir / "cases.jsonl", "w") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")

        print(f"\nArtifacts emitted to {output_dir}/")

    return summary


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run Golden Dataset evaluation suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run evaluation using file-based golden dataset
  %(prog)s --golden-only

  # Validate golden dataset without running agent
  %(prog)s --check-only

  # Run with specific dataset mode
  %(prog)s --dataset synthetic --golden-only

  # Filter by category and difficulty
  %(prog)s --golden-only --category aggregation --difficulty easy
""",
    )
    parser.add_argument("--tenant-id", type=int, default=1, help="Tenant ID")
    parser.add_argument("--category", type=str, help="Filter by category")
    parser.add_argument("--difficulty", type=str, help="Filter by difficulty")
    parser.add_argument(
        "--dataset",
        choices=["synthetic", "pagila"],
        default=None,
        help="Dataset mode (default: from DATASET_MODE env var)",
    )
    parser.add_argument(
        "--golden-only",
        action="store_true",
        help="Use file-based golden dataset instead of database",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate golden dataset without running agent (for CI)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load test cases but skip agent execution",
    )

    args = parser.parse_args()

    # Determine dataset mode
    dataset_mode = args.dataset
    if dataset_mode is None:
        from common.config.dataset import get_dataset_mode

        dataset_mode = get_dataset_mode()

    # Check-only mode: just validate the golden dataset
    if args.check_only:
        success = validate_golden_dataset_cli(
            dataset_mode=dataset_mode,
            category=args.category,
            difficulty=args.difficulty,
        )
        sys.exit(0 if success else 1)

    # Run evaluation
    result = asyncio.run(
        run_evaluation_suite(
            tenant_id=args.tenant_id,
            category=args.category,
            difficulty=args.difficulty,
            dataset_mode=dataset_mode,
            golden_only=args.golden_only,
            dry_run=args.dry_run,
        )
    )

    # Exit with error code if any tests failed
    if result and result["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Evaluation runner for Golden Dataset regression testing."""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

# Add agent to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agent_core.graph import run_agent_with_tracing  # noqa: E402

load_dotenv()


async def fetch_test_cases(tenant_id: int = 1, category: str = None):
    """Fetch active test cases from Golden Dataset."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "pagila")
    db_user = os.getenv("DB_USER", "text2sql_ro")
    db_pass = os.getenv("DB_PASS", "secure_agent_pass")

    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_pass,
    )

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


async def execute_ground_truth_sql(sql: str, tenant_id: int):
    """Execute ground truth SQL to get expected result."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "pagila")
    db_user = os.getenv("DB_USER", "text2sql_ro")
    db_pass = os.getenv("DB_PASS", "secure_agent_pass")

    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_pass,
    )

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


async def evaluate_test_case(test_case: dict, tenant_id: int):
    """Evaluate a single test case."""
    test_id = test_case["test_id"]
    question = test_case["question"]
    ground_truth_sql = test_case["ground_truth_sql"]
    expected_row_count = test_case.get("expected_row_count")

    print(f"\n[Test {test_id}] {question}")

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

        # Determine correctness
        is_correct = False
        error_message = None

        if error:
            error_message = error
        elif generated_sql and actual_result is not None:
            # Execute ground truth SQL to compare
            try:
                expected_result = await execute_ground_truth_sql(ground_truth_sql, tenant_id)

                # Functional correctness: compare actual results
                # For now, compare row counts and first row values
                actual_row_count = len(actual_result)
                expected_row_count_actual = len(expected_result)

                if expected_row_count is not None:
                    is_correct = actual_row_count == expected_row_count
                else:
                    # Compare first row if available
                    if actual_result and expected_result:
                        # Simple comparison: check if first row matches
                        # In production, use more sophisticated comparison
                        is_correct = (
                            actual_row_count == expected_row_count_actual
                            and actual_result[0] == expected_result[0]
                        )
                    else:
                        is_correct = actual_row_count == expected_row_count_actual
            except Exception as e:
                error_message = f"Ground truth execution failed: {str(e)}"

        # Get token count from MLflow trace (simplified - would need trace lookup)
        token_count = None  # TODO: Extract from MLflow trace

        # Store evaluation result
        await store_evaluation_result(
            test_id=test_id,
            generated_sql=generated_sql,
            actual_result=actual_result,
            actual_row_count=len(actual_result) if actual_result else 0,
            is_correct=is_correct,
            error_message=error_message,
            token_count=token_count,
            execution_time_ms=execution_time_ms,
            tenant_id=tenant_id,
        )

        status = "✓ PASS" if is_correct else "✗ FAIL"
        print(
            f"  {status} - Rows: {len(actual_result) if actual_result else 0}, "
            f"Time: {execution_time_ms}ms"
        )

        return {
            "test_id": test_id,
            "is_correct": is_correct,
            "execution_time_ms": execution_time_ms,
        }

    except Exception as e:
        execution_time_ms = int((time.time() - start_time) * 1000)
        error_message = str(e)

        await store_evaluation_result(
            test_id=test_id,
            generated_sql=None,
            actual_result=None,
            actual_row_count=0,
            is_correct=False,
            error_message=error_message,
            token_count=None,
            execution_time_ms=execution_time_ms,
            tenant_id=tenant_id,
        )

        print(f"  ✗ ERROR - {error_message}")
        return {
            "test_id": test_id,
            "is_correct": False,
            "execution_time_ms": execution_time_ms,
        }


async def store_evaluation_result(
    test_id: int,
    generated_sql: str,
    actual_result: list,
    actual_row_count: int,
    is_correct: bool,
    error_message: str,
    token_count: int,
    execution_time_ms: int,
    tenant_id: int,
):
    """Store evaluation result in database."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "pagila")
    db_user = os.getenv("DB_USER", "text2sql_ro")
    db_pass = os.getenv("DB_PASS", "secure_agent_pass")

    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_pass,
    )

    try:
        await conn.execute(
            """
            INSERT INTO evaluation_results (
                test_id, generated_sql, actual_result, actual_row_count,
                is_correct, error_message, token_count, execution_time_ms, tenant_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
            test_id,
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


async def run_evaluation_suite(tenant_id: int = 1, category: str = None):
    """Run full evaluation suite against Golden Dataset."""
    print("=" * 60)
    print("Golden Dataset Evaluation Suite")
    print("=" * 60)

    # Fetch test cases
    test_cases = await fetch_test_cases(tenant_id=tenant_id, category=category)

    if not test_cases:
        print("No test cases found.")
        return

    print(f"\nFound {len(test_cases)} test cases to evaluate")

    # Run evaluation
    results = []
    for test_case in test_cases:
        result = await evaluate_test_case(test_case, tenant_id)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("Evaluation Summary")
    print("=" * 60)

    total = len(results)
    passed = sum(1 for r in results if r["is_correct"])
    failed = total - passed
    avg_time = sum(r["execution_time_ms"] for r in results) / total if total > 0 else 0

    print(f"Total Tests: {total}")
    print(f"Passed: {passed} ({passed/total*100:.1f}%)")
    print(f"Failed: {failed} ({failed/total*100:.1f}%)")
    print(f"Average Execution Time: {avg_time:.0f}ms")

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "accuracy": passed / total if total > 0 else 0,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Golden Dataset evaluation suite")
    parser.add_argument("--tenant-id", type=int, default=1, help="Tenant ID")
    parser.add_argument("--category", type=str, help="Filter by category")

    args = parser.parse_args()

    asyncio.run(run_evaluation_suite(tenant_id=args.tenant_id, category=args.category))

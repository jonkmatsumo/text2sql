"""Analysis and metrics for evaluation results."""

import asyncio
import os
from datetime import datetime, timedelta

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def get_evaluation_metrics(tenant_id: int = 1, days: int = 7):
    """Get evaluation metrics for the last N days."""
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
        cutoff_date = datetime.now() - timedelta(days=days)

        # Overall accuracy
        overall = await conn.fetchrow(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as passed,
                AVG(execution_time_ms) as avg_time,
                AVG(token_count) as avg_tokens
            FROM evaluation_results
            WHERE tenant_id = $1 AND run_timestamp >= $2
        """,
            tenant_id,
            cutoff_date,
        )

        # Accuracy by category
        by_category = await conn.fetch(
            """
            SELECT
                gd.category,
                COUNT(*) as total,
                SUM(CASE WHEN er.is_correct THEN 1 ELSE 0 END) as passed,
                AVG(er.execution_time_ms) as avg_time
            FROM evaluation_results er
            JOIN golden_dataset gd ON er.test_id = gd.test_id
            WHERE er.tenant_id = $1 AND er.run_timestamp >= $2
            GROUP BY gd.category
            ORDER BY gd.category
        """,
            tenant_id,
            cutoff_date,
        )

        # Accuracy by difficulty
        by_difficulty = await conn.fetch(
            """
            SELECT
                gd.difficulty,
                COUNT(*) as total,
                SUM(CASE WHEN er.is_correct THEN 1 ELSE 0 END) as passed,
                AVG(er.execution_time_ms) as avg_time
            FROM evaluation_results er
            JOIN golden_dataset gd ON er.test_id = gd.test_id
            WHERE er.tenant_id = $1 AND er.run_timestamp >= $2
            GROUP BY gd.difficulty
            ORDER BY gd.difficulty
        """,
            tenant_id,
            cutoff_date,
        )

        # Recent failures
        recent_failures = await conn.fetch(
            """
            SELECT
                er.evaluation_id,
                gd.question,
                er.error_message,
                er.run_timestamp
            FROM evaluation_results er
            JOIN golden_dataset gd ON er.test_id = gd.test_id
            WHERE er.tenant_id = $1
                AND er.is_correct = false
                AND er.run_timestamp >= $2
            ORDER BY er.run_timestamp DESC
            LIMIT 10
        """,
            tenant_id,
            cutoff_date,
        )

        return {
            "overall": dict(overall),
            "by_category": [dict(row) for row in by_category],
            "by_difficulty": [dict(row) for row in by_difficulty],
            "recent_failures": [dict(row) for row in recent_failures],
        }
    finally:
        await conn.close()


async def print_metrics(tenant_id: int = 1, days: int = 7):
    """Print evaluation metrics."""
    metrics = await get_evaluation_metrics(tenant_id=tenant_id, days=days)

    print("=" * 60)
    print(f"Evaluation Metrics (Last {days} days)")
    print("=" * 60)

    overall = metrics["overall"]
    total = overall["total"] or 0
    passed = overall["passed"] or 0
    accuracy = (passed / total * 100) if total > 0 else 0

    print(f"\nOverall Accuracy: {accuracy:.1f}% ({passed}/{total})")
    print(f"Average Execution Time: {overall['avg_time']:.0f}ms")
    if overall["avg_tokens"]:
        print(f"Average Token Count: {overall['avg_tokens']:.0f}")

    print("\n--- By Category ---")
    for row in metrics["by_category"]:
        cat_accuracy = (row["passed"] / row["total"] * 100) if row["total"] > 0 else 0
        print(
            f"{row['category']:20s} {cat_accuracy:5.1f}% "
            f"({row['passed']}/{row['total']}) - {row['avg_time']:.0f}ms"
        )

    print("\n--- By Difficulty ---")
    for row in metrics["by_difficulty"]:
        diff_accuracy = (row["passed"] / row["total"] * 100) if row["total"] > 0 else 0
        print(
            f"{row['difficulty']:10s} {diff_accuracy:5.1f}% "
            f"({row['passed']}/{row['total']}) - {row['avg_time']:.0f}ms"
        )

    if metrics["recent_failures"]:
        print("\n--- Recent Failures ---")
        for failure in metrics["recent_failures"]:
            print(f"\n[{failure['run_timestamp']}] {failure['question']}")
            if failure["error_message"]:
                print(f"  Error: {failure['error_message']}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="View evaluation metrics")
    parser.add_argument("--tenant-id", type=int, default=1, help="Tenant ID")
    parser.add_argument("--days", type=int, default=7, help="Number of days to analyze")

    args = parser.parse_args()

    asyncio.run(print_metrics(tenant_id=args.tenant_id, days=args.days))

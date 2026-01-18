import argparse
import asyncio
import logging
import sys
from pathlib import Path

from airflow_evals.runner.config import EvaluationConfig
from airflow_evals.runner.core import run_evaluation

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("eval_cli")


async def main():
    """Run the evaluation CLI."""
    parser = argparse.ArgumentParser(description="Text-to-SQL Evaluation Runner")

    # Required arguments
    parser.add_argument("--dataset", required=True, help="Path to the golden dataset JSONL file")
    parser.add_argument("--output-dir", required=True, help="Directory to store artifacts")

    # Optional arguments
    parser.add_argument("--limit", type=int, help="Limit number of cases to run")
    parser.add_argument("--tenant-id", type=int, default=1, help="Tenant ID to use")
    parser.add_argument("--concurrency", type=int, default=1, help="Concurrency level")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--run-id", type=str, help="Custom run ID")

    args = parser.parse_args()

    # Validate dataset exists
    dataset_path = Path(args.dataset).resolve()
    if not dataset_path.exists():
        logger.error(f"Dataset not found: {dataset_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir).resolve()

    # Create config
    config = EvaluationConfig(
        dataset_path=str(dataset_path),
        output_dir=str(output_dir),
        run_id=args.run_id,
        limit=args.limit,
        concurrency=args.concurrency,
        seed=args.seed,
        tenant_id=args.tenant_id,
    )

    try:
        summary = await run_evaluation(config)

        # Print summary table
        print("\n" + "=" * 50)
        print(f"EVALUATION COMPLETE: {summary.run_id}")
        print("=" * 50)
        print(f"Total Cases:      {summary.total_cases}")
        print(f"Successful:       {summary.successful_cases}")
        print(f"Failed:           {summary.failed_cases}")
        print(f"Accuracy:         {summary.accuracy:.2%}")
        print(f"Avg Latency:      {summary.avg_latency_ms:.2f}ms")
        print("=" * 50)
        print(f"Artifacts: {output_dir}/{summary.run_id}")

    except Exception:
        logger.exception("Evaluation failed")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

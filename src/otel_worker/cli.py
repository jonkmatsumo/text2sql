import argparse
import logging
import sys

from otel_worker.metrics.aggregate import run_aggregation
from otel_worker.storage.postgres import engine
from otel_worker.storage.reconciliation import run_reconciliation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Run the OTEL worker CLI."""
    parser = argparse.ArgumentParser(description="OTEL Worker Management CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Aggregate Command
    agg_parser = subparsers.add_parser("aggregate", help="Run metrics aggregation job")
    agg_parser.add_argument(
        "--lookback",
        type=int,
        default=60,
        help="Lookback window in minutes for traces (default: 60)",
    )
    agg_parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing (default: 100)",
    )

    # Reconcile Command
    rec_parser = subparsers.add_parser("reconcile", help="Cleanup orphan MinIO objects")
    rec_parser.add_argument(
        "--age",
        type=int,
        default=60,
        help="Min age in minutes for orphans (default: 60)",
    )

    args = parser.parse_args()

    if args.command == "aggregate":
        logger.info(f"Starting aggregation (Lookback: {args.lookback}m)")
        try:
            stats = run_aggregation(
                engine, lookback_minutes=args.lookback, batch_size=args.batch_size
            )
            logger.info(f"Aggregation finished successfully: {stats}")
        except Exception as e:
            logger.error(f"Aggregation failed: {e}", exc_info=True)
            sys.exit(1)
    elif args.command == "reconcile":
        try:
            stats = run_reconciliation(engine, age_minutes=args.age)
            logger.info(f"Reconciliation finished: {stats}")
        except Exception as e:
            logger.error(f"Reconciliation failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

"""CLI script to compute observability regressions."""

import argparse
import logging
import os
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "observability/otel-worker/src"))

from otel_worker.metrics.regression import compute_regressions  # noqa: E402
from otel_worker.storage.postgres import engine  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("compute_regressions")


def main():
    """Run regression computation."""
    parser = argparse.ArgumentParser(description="Compute observability regressions.")
    parser.add_argument(
        "--candidate-minutes", type=int, default=30, help="Candidate window size in minutes"
    )
    parser.add_argument(
        "--baseline-minutes", type=int, default=30, help="Baseline window size in minutes"
    )
    parser.add_argument(
        "--offset-minutes",
        type=int,
        default=30,
        help="Offset for baseline window (e.g. 30 for previous window, 1440 for yesterday)",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with non-zero code if regression found",
    )

    args = parser.parse_args()

    try:
        regressions = compute_regressions(
            engine=engine,
            candidate_minutes=args.candidate_minutes,
            baseline_minutes=args.baseline_minutes,
            offset_minutes=args.offset_minutes,
        )

        if regressions > 0:
            print(f"FAIL: {regressions} regressions detected.")
            if args.fail_on_regression:
                sys.exit(1)
        else:
            print("PASS: No regressions detected.")

    except Exception as e:
        logger.error(f"Error computing regressions: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()

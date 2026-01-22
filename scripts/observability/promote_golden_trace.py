#!/usr/bin/env python3
"""Script to promote a trace to golden status."""

import argparse
import json
import logging
import os
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "observability/otel-worker/src"))

from sqlalchemy import text  # noqa: E402

from otel_worker.storage.postgres import engine  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("promote_golden_trace")


def promote_trace(trace_id: str, reason: str, promoted_by: str, labels: dict):
    """Promote a trace to golden status."""
    with engine.begin() as conn:
        # Check if trace exists in otel.traces
        res = conn.execute(
            text("SELECT 1 FROM otel.traces WHERE trace_id = :tid"), {"tid": trace_id}
        ).fetchone()

        if not res:
            logger.error(f"Trace {trace_id} not found in otel.traces.")
            sys.exit(1)

        conn.execute(
            text(
                """
                INSERT INTO otel.golden_traces (trace_id, reason, promoted_by, labels, promoted_at)
                VALUES (:tid, :reason, :by, :labels, NOW())
                ON CONFLICT (trace_id) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    promoted_by = EXCLUDED.promoted_by,
                    labels = EXCLUDED.labels,
                    promoted_at = NOW()
            """
            ),
            {
                "tid": trace_id,
                "reason": reason,
                "by": promoted_by,
                "labels": json.dumps(labels),
                # SQLAlchemy + psycopg2 handles JSONB often via string or dict depending on driver,
                # strict JSON string is safest
            },
        )
        logger.info(f"Trace {trace_id} promoted to golden. Reason: {reason}")


def main():
    """Run promotion script."""
    parser = argparse.ArgumentParser(description="Promote a trace to golden status.")
    parser.add_argument("trace_id", help="Trace ID to promote")
    parser.add_argument("--reason", required=True, help="Reason for promotion")
    parser.add_argument(
        "--promoted-by", default=os.environ.get("USER", "unknown"), help="User promoting the trace"
    )
    parser.add_argument(
        "--labels",
        help="JSON string or comma-separated k=v pairs (e.g. 'type=benchmark,quality=high')",
    )

    args = parser.parse_args()

    labels = {}
    if args.labels:
        try:
            labels = json.loads(args.labels)
        except json.JSONDecodeError:
            # Try k=v parsing
            try:
                for pair in args.labels.split(","):
                    k, v = pair.split("=")
                    labels[k.strip()] = v.strip()
            except ValueError:
                logger.error("Invalid labels format. Use JSON or k=v,k=v")
                sys.exit(1)

    try:
        promote_trace(args.trace_id, args.reason, args.promoted_by, labels)
    except Exception as e:
        logger.error(f"Error promoting trace: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

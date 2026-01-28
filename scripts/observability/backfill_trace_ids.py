#!/usr/bin/env python3
"""Backfill query_interactions.trace_id from otel.traces.interaction_id.

Assumes OTEL schema and control-plane tables exist in the same Postgres database.
"""

import argparse
import os

from sqlalchemy import create_engine, text


def get_db_url() -> str:
    """Resolve the Postgres connection string."""
    db_url = os.getenv("POSTGRES_URL") or os.getenv("POSTGRES_CONNECTION_STRING")
    if not db_url:
        raise RuntimeError("POSTGRES_URL or POSTGRES_CONNECTION_STRING must be set")
    return db_url


def backfill(dry_run: bool = True) -> int:
    """Backfill trace ids and return affected row count."""
    engine = create_engine(get_db_url())
    with engine.begin() as conn:
        if dry_run:
            result = conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM query_interactions qi
                    JOIN otel.traces t ON t.interaction_id = qi.id::text
                    WHERE qi.trace_id IS NULL OR qi.trace_id = ''
                    """
                )
            )
            return result.scalar() or 0

        result = conn.execute(
            text(
                """
                UPDATE query_interactions qi
                SET trace_id = t.trace_id
                FROM otel.traces t
                WHERE t.interaction_id = qi.id::text
                  AND (qi.trace_id IS NULL OR qi.trace_id = '')
                RETURNING qi.id
                """
            )
        )
        return result.rowcount or 0


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Backfill query_interactions.trace_id from otel.traces."
    )
    parser.add_argument("--dry-run", action="store_true", help="Only report counts")
    args = parser.parse_args()

    updated = backfill(dry_run=args.dry_run)
    if args.dry_run:
        print(f"Would update {updated} interactions.")
    else:
        print(f"Updated {updated} interactions.")


if __name__ == "__main__":
    main()

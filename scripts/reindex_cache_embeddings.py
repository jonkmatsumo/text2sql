#!/usr/bin/env python3
"""Reindex cache embeddings with canonical representation.

This script migrates existing cache entries to use canonical embedding inputs
that prominently include hard constraints (rating, limit, etc.) for better
similarity separation.

Usage:
    docker exec text2sql_mcp python /app/scripts/reindex_cache_embeddings.py [--dry-run]
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "mcp-server" / "src"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def reindex_embeddings(dry_run: bool = True) -> dict:
    """Reindex all cache embeddings with canonical representation.

    Args:
        dry_run: If True, only report what would be done without making changes.

    Returns:
        dict with count of processed entries and any errors.
    """
    # Import here to avoid circular imports
    from mcp_server.config.database import Database
    from mcp_server.rag import RagEngine

    results = {"processed": 0, "errors": 0, "skipped": 0}

    # Query all cache entries
    query = """
        SELECT cache_id, tenant_id, user_query, generated_sql
        FROM semantic_cache
        WHERE is_tombstoned IS NULL OR is_tombstoned = FALSE
        ORDER BY cache_id
    """

    try:
        async with Database.get_connection() as conn:
            rows = await conn.fetch(query)
            logger.info(f"Found {len(rows)} cache entries to process")

            for row in rows:
                cache_id = row["cache_id"]
                user_query = row["user_query"]
                tenant_id = row["tenant_id"]

                try:
                    # Build canonical embedding input
                    # Import constraint extractor (from agent, so adjust path if needed)
                    from agent_core.cache import extract_constraints

                    constraints = extract_constraints(user_query)
                    canonical = build_canonical_input(user_query, constraints)

                    if dry_run:
                        logger.info(
                            f"[DRY RUN] Would reindex cache_id={cache_id}: "
                            f"{user_query[:50]}... -> {canonical[:50]}..."
                        )
                    else:
                        # Generate new embedding
                        new_embedding = RagEngine.embed_text(canonical)

                        # Update in database
                        from mcp_server.dal.postgres.common import _format_vector

                        pg_vector = _format_vector(new_embedding)
                        update_query = """
                            UPDATE semantic_cache
                            SET query_embedding = $1
                            WHERE cache_id = $2 AND tenant_id = $3
                        """
                        await conn.execute(update_query, pg_vector, cache_id, tenant_id)
                        logger.info(f"Reindexed cache_id={cache_id}")

                    results["processed"] += 1

                except Exception as e:
                    logger.error(f"Error processing cache_id={cache_id}: {e}")
                    results["errors"] += 1

    except Exception as e:
        logger.error(f"Database error: {e}")
        raise

    return results


def build_canonical_input(query: str, constraints) -> str:
    """Build canonical embedding input from query and constraints.

    Args:
        query: Original user query.
        constraints: Extracted QueryConstraints.

    Returns:
        Canonical string with hard constraints prominently included.
    """
    parts = []
    if constraints.rating:
        parts.append(f"rating={constraints.rating}")
    if constraints.limit:
        parts.append(f"limit={constraints.limit}")
    if constraints.entity:
        parts.append(f"entity={constraints.entity}")
    parts.append(f"query={query}")
    return "; ".join(parts)


def main():
    """Run the reindex script."""
    parser = argparse.ArgumentParser(
        description="Reindex cache embeddings with canonical representation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Only show what would be done (default: True)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually execute the reindex (disables dry-run)",
    )

    args = parser.parse_args()
    dry_run = not args.execute

    logger.info(f"Starting cache reindex {'(DRY RUN)' if dry_run else '(EXECUTING)'}")

    results = asyncio.run(reindex_embeddings(dry_run=dry_run))

    logger.info(
        f"Reindex complete: {results['processed']} processed, "
        f"{results['errors']} errors, {results['skipped']} skipped"
    )

    sys.exit(1 if results["errors"] > 0 else 0)


if __name__ == "__main__":
    main()

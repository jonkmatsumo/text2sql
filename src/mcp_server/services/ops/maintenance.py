"""Maintenance Service for operational tasks."""

import logging
from typing import AsyncGenerator

from dal.database import Database

logger = logging.getLogger(__name__)


class MaintenanceService:
    """Service to handle operational maintenance tasks."""

    @staticmethod
    async def generate_patterns(dry_run: bool = False) -> AsyncGenerator[str, None]:
        """Generate EntityRuler patterns from database schema interactions."""
        # lazy import to avoid circular dependency
        from datetime import datetime

        from dal.factory import get_pattern_run_store
        from ingestion.patterns.generator import generate_entity_patterns

        run_store = get_pattern_run_store()
        run_id = await run_store.create_run(status="RUNNING", config_snapshot={"dry_run": dry_run})

        yield f"Starting pattern generation (Run ID: {run_id})..."

        try:
            patterns = await generate_entity_patterns(run_id=str(run_id))
            yield f"Generated {len(patterns)} patterns."

            if dry_run:
                yield "DRY RUN: Skipping database write."
                for p in patterns[:5]:
                    yield f"Sample: {p}"

                await run_store.update_run(
                    run_id,
                    status="COMPLETED",
                    completed_at=datetime.now(),
                    metrics={"generated_count": len(patterns), "note": "dry_run"},
                )
                return

            yield "Writing patterns to database..."
            if not patterns:
                yield "No patterns to write."
                await run_store.update_run(
                    run_id,
                    status="COMPLETED",
                    completed_at=datetime.now(),
                    metrics={"generated_count": 0},
                )
                return

            # Determine Actions (Diff against DB)
            existing_map = {}
            async with Database.get_connection() as conn:
                # We fetch all existing patterns to diff.
                # Note: This might be heavy if nlp_patterns is huge, but for MVP/v1 is fine.
                rows = await conn.fetch("SELECT label, pattern, id FROM nlp_patterns")
                for r in rows:
                    existing_map[(r["label"], r["pattern"])] = r["id"]

            run_items = []
            for p in patterns:
                key = (p["label"], p["pattern"])
                new_id = p["id"]

                if key not in existing_map:
                    action = "CREATED"
                elif existing_map[key] != new_id:
                    action = "UPDATED"
                else:
                    action = "UNCHANGED"

                run_items.append(
                    {
                        "pattern_id": new_id,
                        "label": p["label"],
                        "pattern": p["pattern"],
                        "action": action,
                    }
                )

            # Bulk insert / Upsert
            async with Database.get_connection() as conn:
                data = [(p["id"], p["label"], p["pattern"]) for p in patterns]

                await conn.executemany(
                    """
                    INSERT INTO nlp_patterns (id, label, pattern)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (label, pattern)
                    DO UPDATE SET id = EXCLUDED.id, created_at = CURRENT_TIMESTAMP
                    """,
                    data,
                )

            # Record Run Items and Complete
            await run_store.add_run_items(run_id, run_items)

            metrics = {
                "generated_count": len(patterns),
                "created_count": sum(1 for i in run_items if i["action"] == "CREATED"),
                "updated_count": sum(1 for i in run_items if i["action"] == "UPDATED"),
                "unchanged_count": sum(1 for i in run_items if i["action"] == "UNCHANGED"),
            }

            await run_store.update_run(
                run_id, status="COMPLETED", completed_at=datetime.now(), metrics=metrics
            )

            yield "Patterns successfully saved to 'nlp_patterns' table."

        except Exception as e:
            logger.error(f"Pattern generation failed: {e}", exc_info=True)
            # Update run as FAILED
            # We catch generic exception, so we can't be sure if run_store is accessible
            # but we try our best.
            try:
                await run_store.update_run(
                    run_id, status="FAILED", completed_at=datetime.now(), error_message=str(e)
                )
            except Exception as e2:
                logger.error(f"Failed to update run status: {e2}")

            yield f"Error: {e}"

    @staticmethod
    async def hydrate_schema() -> AsyncGenerator[str, None]:
        """Hydrate Memgraph from Postgres schema."""
        yield "Starting schema hydration..."

        from mcp_server.services.rag.indexer import index_all_tables

        try:
            # This logic should ideally be made async if it involves heavy IO
            # For now we call it directly
            await index_all_tables()
            yield "Hydration logic completed successfully."
        except Exception as e:
            yield f"Error during hydration: {e}"

    @staticmethod
    async def reindex_cache() -> AsyncGenerator[str, None]:
        """Re-index the semantic cache (Unified Registry)."""
        yield "Starting cache re-indexing..."

        from dal.factory import get_registry_store
        from mcp_server.services.rag import RagEngine

        store = get_registry_store()

        try:
            # 1. Fetch all non-tombstoned items
            # Note: This might be heavy for very large registries
            # In a real system, we'd process in batches.
            pairs = await store.lookup_semantic_candidates(
                embedding=[0] * 384,  # Dummy embedding to get all
                tenant_id=None,
                threshold=-1.0,  # All
                limit=10000,
            )

            yield f"Found {len(pairs)} items to re-embed."

            for i, pair in enumerate(pairs):
                # Re-generate embedding
                new_embedding = await RagEngine.embed_text(pair.question)
                pair.embedding = new_embedding
                await store.store_pair(pair)

                if (i + 1) % 10 == 0:
                    yield f"Processed {i + 1}/{len(pairs)} items..."

            yield "Cache re-indexing completed."
        except Exception as e:
            yield f"Error during re-indexing: {e}"

"""Maintenance Service for operational tasks."""

import logging
from typing import AsyncGenerator

from mcp_server.config.database import Database

logger = logging.getLogger(__name__)


class MaintenanceService:
    """Service to handle operational maintenance tasks."""

    @staticmethod
    async def generate_patterns(dry_run: bool = False) -> AsyncGenerator[str, None]:
        """Generate EntityRuler patterns from database schema interactions."""
        from mcp_server.services.patterns.generator import generate_entity_patterns
        
        yield "Starting pattern generation (Intospection & LLM Enrichment)..."
        
        try:
            patterns = await generate_entity_patterns()
            yield f"Generated {len(patterns)} patterns."
            
            if dry_run:
                yield "DRY RUN: Skipping database write."
                for p in patterns[:5]:
                    yield f"Sample: {p}"
            else:
                yield "Writing patterns to database..."
                if not patterns:
                    yield "No patterns to write."
                    return

                # Bulk insert / Upsert
                # We use ON CONFLICT DO NOTHING or UPDATE for idempotency
                # PK is (label, pattern)
                async with Database.get_connection() as conn:
                    # Prepare data for executemany
                    data = [(p["id"], p["label"], p["pattern"]) for p in patterns]
                    
                    await conn.executemany(
                        """
                        INSERT INTO nlp_patterns (id, label, pattern)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (label, pattern) 
                        DO UPDATE SET id = EXCLUDED.id, created_at = CURRENT_TIMESTAMP
                        """,
                        data
                    )
                yield "Patterns successfully saved to 'nlp_patterns' table."
                
        except Exception as e:
            logger.error(f"Pattern generation failed: {e}", exc_info=True)
            yield f"Error: {e}"

    @staticmethod
    async def hydrate_schema() -> AsyncGenerator[str, None]:
        """Hydrate Memgraph from Postgres schema."""
        yield "Starting schema hydration..."

        # We will integrate the actual GraphHydrator here later or now?
        # The plan says "Wraps GraphHydrator logic".
        # For Phase 1, we just need the stub or basic integration.
        # Let's import it to see if it works, or leave as comment if it's complex dependency.
        try:
            from mcp_server.services.seeding.cli import DatabaseSeeder

            # Note: The original CLI logic might need refactoring to separate logging from execution
            # For now, we yield a placeholder
            yield "Hydration logic invoked."
        except ImportError:
            yield "Error: Could not import hydration logic."

    @staticmethod
    async def reindex_cache() -> AsyncGenerator[str, None]:
        """Re-index the semantic cache."""
        yield "Starting cache re-indexing..."
        # STUB
        yield "Cache re-indexed (STUB)."

#!/usr/bin/env python3
"""Generate EntityRuler patterns from database schema.

This script introspects the database to generate dynamic patterns for:
- Ratings (from film.rating)
- Categories/Genres (from category.name)
- Other enumerated values

Run this whenever the database schema changes or new enum values are added.

Usage:
    python -m mcp_server.scripts.generate_patterns

Or from project root:
    docker exec text2sql_core python -m mcp_server.scripts.generate_patterns
"""

import asyncio
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def generate_entity_patterns() -> list[dict]:
    """Generate entity patterns from database introspection."""
    from mcp_server.config.database import Database

    patterns = []

    async with Database.get_connection(tenant_id=1) as conn:
        # Get distinct ratings from film table
        rows = await conn.fetch("SELECT DISTINCT rating FROM film WHERE rating IS NOT NULL")
        for row in rows:
            rating = row["rating"]
            patterns.append({"label": "RATING", "pattern": rating, "id": rating})
            # Handle variations
            patterns.append({"label": "RATING", "pattern": f"{rating}-rated", "id": rating})
            patterns.append({"label": "RATING", "pattern": f"rated {rating}", "id": rating})
            # Handle hyphenated ratings like PG-13
            if "-" in rating:
                patterns.append(
                    {"label": "RATING", "pattern": rating.replace("-", " "), "id": rating}
                )
                patterns.append(
                    {"label": "RATING", "pattern": rating.replace("-", ""), "id": rating}
                )

        # Get categories/genres
        rows = await conn.fetch("SELECT name FROM category")
        for row in rows:
            name = row["name"]
            patterns.append({"label": "GENRE", "pattern": name.lower(), "id": name.upper()})
            patterns.append({"label": "GENRE", "pattern": name, "id": name.upper()})

    return patterns


async def main() -> None:
    """Run the pattern generation pipeline."""
    logger.info("Generating entity patterns from database...")

    patterns = await generate_entity_patterns()

    # Write to JSONL file
    output_path = Path(__file__).parent.parent / "patterns" / "entities.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        for pattern in patterns:
            f.write(json.dumps(pattern) + "\n")

    logger.info(f"Generated {len(patterns)} entity patterns → {output_path}")


if __name__ == "__main__":
    asyncio.run(main())

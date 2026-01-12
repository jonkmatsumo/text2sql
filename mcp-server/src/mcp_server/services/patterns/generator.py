#!/usr/bin/env python3
"""Generate EntityRuler patterns from database schema with LLM enrichment.

This script introspects the database to generate dynamic patterns for:
- Ratings (from film.rating)
- Categories/Genres (from category.name)
- Other enumerated values

It uses an LLM to generate colloquial synonyms for these values.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List

from mcp_server.config.database import Database
from openai import AsyncOpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def get_openai_client() -> AsyncOpenAI:
    """Get AsyncOpenAI client."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("OPENAI_API_KEY not found. LLM enrichment will be skipped.")
        return None
    return AsyncOpenAI(api_key=api_key)


async def enrich_values_with_llm(
    client: AsyncOpenAI, label: str, values: List[str]
) -> List[Dict[str, str]]:
    """Generate synonyms for values using LLM."""
    if not client:
        return []

    patterns = []

    # We process in batches if needed, but for now assuming small list
    prompt = f"""
    You are an expert at generating colloquial synonyms for database values for NLP entity recognition.

    For the Entity Label "{label}", generate 3-5 colloquial synonyms for EACH of the following canonical values.

    Canonical Values: {json.dumps(values)}

    Return ONLY a valid JSON array of objects with the following format:
    [{{"pattern": "synonym", "id": "CANONICAL_VALUE"}}, ...]

    Example for RATING "G":
    [{{"pattern": "general audience", "id": "G"}}, {{"pattern": "kids movie", "id": "G"}}]

    Do not include the original values in the output, only new variations.
    Lowercase all patterns.
    """

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",  # Use a cheap/fast model
            messages=[
                {"role": "system", "content": "You are a helpful data assistant."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.3,
        )
        content = response.choices[0].message.content
        data = json.loads(content)

        # Handle cases where LLM might wrap result in a key
        if isinstance(data, dict):
            # look for a list value
            for key, val in data.items():
                if isinstance(val, list):
                    generated = val
                    break
            else:
                generated = []
        elif isinstance(data, list):
            generated = data
        else:
            generated = []

        # Validate and add label
        for item in generated:
            if "pattern" in item and "id" in item:
                patterns.append(
                    {"label": label, "pattern": item["pattern"].lower(), "id": item["id"]}
                )

    except Exception as e:
        logger.error(f"Error generating synonyms for {label}: {e}")

    return patterns


async def generate_entity_patterns() -> list[dict]:
    """Generate entity patterns from database introspection."""

    # Initialize DB specifically for this script if not already initialized
    # check if pool exists, if not init.
    # But Database.get_connection raises if not init.
    # So we must verify connectivity or assume init is called by caller.
    # Since this is a library function now called by MaintenanceService,
    # we assume Database.init() is handled there.
    # However, for the __main__ block, we need to init.

    patterns = []

    # We assume Database is initialized.

    client = await get_openai_client()

    async with Database.get_connection(tenant_id=1) as conn:
        # 1. Ratings
        logger.info("Introspecting ratings...")
        rows = await conn.fetch("SELECT DISTINCT rating FROM film WHERE rating IS NOT NULL")
        ratings = [row["rating"] for row in rows]

        # Add exact matches
        for r in ratings:
            patterns.append({"label": "RATING", "pattern": r, "id": r})
            # Basic heuristic variations
            patterns.append({"label": "RATING", "pattern": f"{r} rated", "id": r})
            patterns.append({"label": "RATING", "pattern": f"rated {r}", "id": r})

        # LLM Enrichment for Ratings
        if client:
            logger.info("Enriching ratings with LLM...")
            synonyms = await enrich_values_with_llm(client, "RATING", ratings)
            patterns.extend(synonyms)

        # 2. Categories / Genres
        logger.info("Introspecting categories...")
        rows = await conn.fetch("SELECT name FROM category")
        genres = [row["name"] for row in rows]

        for g in genres:
            patterns.append({"label": "GENRE", "pattern": g.lower(), "id": g.upper()})
            patterns.append({"label": "GENRE", "pattern": g, "id": g.upper()})

        # LLM Enrichment for Genres
        if client:
            logger.info("Enriching genres with LLM...")
            synonyms = await enrich_values_with_llm(client, "GENRE", genres)
            # Ensure mapped IDs are consistent.
            # ID field for genre should be the Name itself or uppercase Name.
            # The manual code used g.upper() as id. The LLM returns 'id'.

            # Let's fix the IDs in the loop
            for p in synonyms:
                p["id"] = p["id"].upper()  # Enforce uppercase ID for genres

            patterns.extend(synonyms)

    return patterns


async def main() -> None:
    """Run the pattern generation pipeline (Standalone Mode)."""
    # Standalone initialization
    try:
        await Database.init()

        patterns = await generate_entity_patterns()

        # For standalone script, we might still want to write to file or stdout
        output_path = "generated_patterns.jsonl"
        with open(output_path, "w") as f:
            for p in patterns:
                f.write(json.dumps(p) + "\n")

        logger.info(f"Generated {len(patterns)} patterns to {output_path}")

    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(main())

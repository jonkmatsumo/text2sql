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
from typing import Dict, List

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.schema_introspector import SchemaIntrospector
from mcp_server.models import ColumnDef
from openai import AsyncOpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_name(name: str) -> List[str]:
    """Normalize a schema name into natural language variations.

    Args:
        name: The raw table or column name (e.g., "user_account", "customer_id").

    Returns:
        A list of natural language variations (e.g., ["user account", "user accounts"]).
    """
    variations = set()

    # 1. Basic snake_case to space
    spaced = name.replace("_", " ")
    variations.add(spaced)
    variations.add(name)  # Keep original just in case

    # 2. ID Handling
    # "user_id" -> "user id", "user identifier"
    if name.endswith("_id"):
        base = name[:-3]
        base_spaced = base.replace("_", " ")
        variations.add(f"{base_spaced} id")
        variations.add(f"{base_spaced} identifier")

    # 3. Simple Pluralization Heuristic (since inflect is not guaranteed)
    # This is rough but covers common cases: add 's', 'es'.
    # We apply this to the "spaced" version.
    # e.g. "user account" -> "user accounts"
    if not (spaced.endswith("s") or spaced.endswith("y")):
        variations.add(f"{spaced}s")

    # Handle 'y' -> 'ies' if needed, though often schema names are singular.
    if (
        spaced.endswith("y")
        and not spaced.endswith("ay")
        and not spaced.endswith("ey")
        and not spaced.endswith("oy")
        and not spaced.endswith("uy")
    ):
        variations.add(f"{spaced[:-1]}ies")

    return list(variations)


async def get_target_tables(introspector: SchemaIntrospector) -> List[str]:
    """Get list of tables to scan, filtering out technical/system tables."""
    # Denylist for system/migration tables
    DENYLIST = {
        "alembic_version",
        "flyway_schema_history",
        "spatial_ref_sys",
        "nlp_patterns",
        "geometry_columns",
        "geography_columns",
        "raster_columns",
        "raster_overviews",
    }

    tables = await introspector.list_table_names()
    return [t for t in tables if t not in DENYLIST]


def generate_table_patterns(table_name: str) -> List[Dict[str, str]]:
    """Generate entity patterns for a table."""
    patterns = []
    # ID is the table name itself
    canonical_id = table_name

    variations = normalize_name(table_name)
    for v in variations:
        patterns.append({"label": "TABLE", "pattern": v.lower(), "id": canonical_id})
    return patterns


def generate_column_patterns(table_name: str, column: ColumnDef) -> List[Dict[str, str]]:
    """Generate entity patterns for a column."""
    patterns = []
    # Canonical ID: table.column (unambiguous)
    canonical_id = f"{table_name}.{column.name}"

    variations = normalize_name(column.name)
    for v in variations:
        patterns.append({"label": "COLUMN", "pattern": v.lower(), "id": canonical_id})

    return patterns


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
    You are an expert at generating colloquial synonyms for database values for NLP
    entity recognition.
    For the Entity Label "{label}", generate 3-5 colloquial synonyms for EACH of the following
    canonical values.

    Canonical Values: {json.dumps(values)}

    Return ONLY a valid JSON array of objects with the following format:
    [
        {{ "pattern": "synonym", "id": "CANONICAL_VALUE" }}, ...
    ]

    Example for RATING "G":
    [
        {{ "pattern": "general audience", "id": "G" }},
        {{ "pattern": "kids movie", "id": "G" }}
    ]

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
    # Initialize DB (assumes already running or managed by caller/main)
    # But for safety in standalone we rely on main, in service we rely on service init.

    patterns = []
    client = await get_openai_client()
    introspector = Database.get_schema_introspector()

    try:
        target_tables = await get_target_tables(introspector)
        logger.info(f"Discovered {len(target_tables)} tables for pattern generation.")

        async with Database.get_connection(tenant_id=1) as conn:
            for table_name in target_tables:
                # 1. Table Patterns
                patterns.extend(generate_table_patterns(table_name))

                # 2. Inspect Table
                try:
                    table_def = await introspector.get_table_def(table_name)
                except Exception as e:
                    logger.warning(f"Could not fetch definition for {table_name}: {e}")
                    continue

                for col in table_def.columns:
                    # 3. Column Patterns
                    patterns.extend(generate_column_patterns(table_name, col))

                    # 4. Value Discovery
                    if should_scan_column(col):
                        logger.info(f"Scanning values for {table_name}.{col.name}...")
                        values = await fetch_distinct_values(conn, table_name, col.name)

                        # Use column name as Label (e.g. status -> STATUS)
                        label = col.name.upper()

                        # Add patterns for values
                        for v in values:
                            # Basic pattern: value itself
                            patterns.append({"label": label, "pattern": v.lower(), "id": v})
                            # Should we add exact case ID? Yes, `v` is exact.

                        # LLM Enrichment
                        if client and values:
                            logger.info(f"Enriching {label} with LLM...")
                            synonyms = await enrich_values_with_llm(client, label, values)
                            patterns.extend(synonyms)

    except Exception as e:
        logger.error(f"Error in pattern generation: {e}")
        # Don't crash entirely, return what we have? OR re-raise?
        # Re-raising is probably better to signal failure.
        raise e

    return patterns


def should_scan_column(column: ColumnDef) -> bool:
    """Determine if a column should be scanned for distinct values."""
    # 1. User-defined types (Enums)
    if column.data_type == "USER-DEFINED":
        return True

    # 2. Textual types only for heuristics
    # (avoid scanning blovs, arrays, numerics unless specific)
    text_types = {"text", "character varying", "varchar", "char", "character", "string"}
    if column.data_type.lower() not in text_types:
        return False

    # 3. Name Heuristics (Whitelist)
    SCAN_KEYWORDS = {
        "status",
        "type",
        "category",
        "genre",
        "rating",
        "payment_method",
        "frequency",
        "kind",
        "level",
        "tier",
        "mode",
    }

    # Check exact match or suffix match (e.g. "payment_status")
    name_lower = column.name.lower()
    if name_lower in SCAN_KEYWORDS:
        return True

    for kw in SCAN_KEYWORDS:
        if name_lower.endswith(f"_{kw}"):
            return True

    return False


async def fetch_distinct_values(
    conn, table_name: str, column_name: str, limit: int = 50
) -> List[str]:
    """Fetch distinct values for a column."""
    try:
        # Safe quoting
        query = (
            f'SELECT DISTINCT "{column_name}" FROM "{table_name}" '
            f'WHERE "{column_name}" IS NOT NULL LIMIT $1'
        )
        rows = await conn.fetch(query, limit)
        return [str(row[0]) for row in rows if row[0] is not None]
    except Exception as e:
        logger.warning(f"Failed to fetch values for {table_name}.{column_name}: {e}")
        return []


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

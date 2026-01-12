#!/usr/bin/env python3
"""Generate EntityRuler patterns from database schema with LLM enrichment.

This script introspects the database to generate dynamic patterns for:
- Ratings (from film.rating)
- Categories/Genres (from category.name)
- Other enumerated values

It uses an LLM to generate colloquial synonyms for these values.

Configuration (Environment Variables):
- ENUM_CARDINALITY_THRESHOLD: Max distinct values to treat as enum (default 10).
- ENUM_CARDINALITY_SAMPLE_ROWS: Number of rows to sample for detection (default 10000).
- ENUM_CARDINALITY_QUERY_TIMEOUT_MS: Timeout for detection queries (default 2000).
- ENUM_VALUE_ALLOWLIST: Comma-separated list of patterns to force include
  (e.g. "users.status, *.type").
- ENUM_VALUE_DENYLIST: Comma-separated list of patterns to force exclude
  (e.g. "users.secret, *.uuid").
- OPENAI_API_KEY: Required for LLM enrichment.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

from mcp_server.config.database import Database
from mcp_server.dal.interfaces.schema_introspector import SchemaIntrospector
from mcp_server.models import ColumnDef
from mcp_server.services.patterns.enum_detector import EnumLikeColumnDetector
from mcp_server.services.patterns.validator import PatternValidator
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
    client: AsyncOpenAI, label: str, values: List[str], run_id: Optional[str] = None
) -> List[Dict[str, str]]:
    """Generate synonyms for values using LLM with retry."""
    if not client:
        return []

    patterns = []
    max_retries = 3
    base_delay = 1.0

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

    for attempt in range(max_retries):
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
                        {
                            "label": label,
                            "pattern": item["pattern"].lower(),
                            "id": item["id"],
                        }
                    )

            # If successful, break retry loop
            if attempt > 0:
                logger.info(f"[{run_id}] Retry validation successful for {label}")
            break

        except Exception as e:
            logger.warning(
                f"[{run_id}] Error generating synonyms for {label} "
                f"(Attempt {attempt + 1}/{max_retries}): {e}"
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2**attempt))
            else:
                logger.error(f"[{run_id}] Failed to generate synonyms for {label} after retries.")

    return patterns


async def get_native_enum_values(conn, table_name: str, column_name: str) -> List[str]:
    """Fetch values for a native ENUM column from system catalog."""
    try:
        # Postgres catalog query
        query = """
            SELECT e.enumlabel
            FROM pg_catalog.pg_enum e
            JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
            JOIN pg_catalog.pg_attribute a ON t.oid = a.atttypid
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            WHERE c.relname = $1 AND a.attname = $2
            ORDER BY e.enumsortorder
        """
        rows = await conn.fetch(query, table_name, column_name)
        return [str(row[0]) for row in rows]
    except Exception as e:
        logger.warning(f"Failed to fetch native enum values for {table_name}.{column_name}: {e}")
        return []


async def sample_distinct_values(
    conn,
    table_name: str,
    column_name: str,
    threshold: int,
    sample_rows: int = 10000,
    timeout_ms: int = 2000,
) -> List[str]:
    """Fetch distinct values from a sample of rows with early stop."""
    try:
        # DB-agnostic-ish SQL (Postgres compatible)
        # WITH sample AS (SELECT col FROM table LIMIT sample_rows)
        # SELECT DISTINCT col FROM sample WHERE col IS NOT NULL LIMIT threshold + 1
        query = (
            f'WITH sample AS (SELECT "{column_name}" FROM "{table_name}" LIMIT {sample_rows}) '
            f'SELECT DISTINCT "{column_name}" FROM sample '
            f'WHERE "{column_name}" IS NOT NULL '
            f"LIMIT {threshold + 1}"
        )

        # Attempt to set local timeout if supported (asyncpg/postgres)
        try:
            await conn.execute(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
        except Exception:
            pass  # Ignore if not supported or not a transaction

        rows = await conn.fetch(query)
        return [str(row[0]) for row in rows if row[0] is not None]
    except Exception as e:
        logger.warning(f"Failed to sample values for {table_name}.{column_name}: {e}")
        return []


async def generate_entity_patterns(run_id: Optional[str] = None) -> list[dict]:
    """Generate entity patterns from database introspection."""
    # Initialize DB (assumes already running or managed by caller/main)
    trusted_patterns = []
    untrusted_patterns = []
    client = await get_openai_client()
    introspector = Database.get_schema_introspector()

    # Initialize Detector
    threshold = int(os.getenv("ENUM_CARDINALITY_THRESHOLD", "10"))

    # Parse Allow/Deny Lists
    allowlist_str = os.getenv("ENUM_VALUE_ALLOWLIST", "")
    denylist_str = os.getenv("ENUM_VALUE_DENYLIST", "")

    allowlist = [s.strip() for s in allowlist_str.split(",") if s.strip()]
    denylist = [s.strip() for s in denylist_str.split(",") if s.strip()]

    detector = EnumLikeColumnDetector(
        threshold=threshold,
        allowlist=allowlist,
        denylist=denylist,
    )

    try:
        target_tables = await get_target_tables(introspector)
        logger.info(f"Discovered {len(target_tables)} tables for pattern generation.")

        async with Database.get_connection(tenant_id=1) as conn:
            for table_name in target_tables:
                # 1. Table Patterns
                trusted_patterns.extend(generate_table_patterns(table_name))

                # 2. Inspect Table
                try:
                    table_def = await introspector.get_table_def(table_name)
                except Exception as e:
                    logger.warning(f"Could not fetch definition for {table_name}: {e}")
                    continue

                for col in table_def.columns:
                    # 3. Column Patterns
                    trusted_patterns.extend(generate_column_patterns(table_name, col))

                    # 4. Value Discovery
                    if detector.is_candidate(table_name, col):
                        values = []
                        is_native = False

                        # 4a. Native Enum Check
                        if col.data_type == "USER-DEFINED":
                            values = await get_native_enum_values(conn, table_name, col.name)
                            if values:
                                is_native = True
                                logger.info(f"Found native enum values for {table_name}.{col.name}")

                        # 4b. Low Cardinality Detection (Scanning)
                        if not values and not is_native:
                            logger.info(
                                f"Scanning values for {table_name}.{col.name} (Candidate)..."
                            )
                            values = await sample_distinct_values(
                                conn,
                                table_name,
                                col.name,
                                threshold=detector.threshold,
                                sample_rows=int(os.getenv("ENUM_CARDINALITY_SAMPLE_ROWS", "10000")),
                                timeout_ms=int(
                                    os.getenv("ENUM_CARDINALITY_QUERY_TIMEOUT_MS", "2000")
                                ),
                            )

                            # Threshold Check (scanned only)
                            if len(values) > detector.threshold:
                                logger.info(
                                    f"Skipping {table_name}.{col.name}: High cardinality "
                                    f"({len(values)} > {detector.threshold})"
                                )
                                continue

                        if not values:
                            continue

                        # Canonicalize
                        values = detector.canonicalize_values(values)

                        # Use column name as Label (e.g. status -> STATUS)
                        label = col.name.upper()

                        # Add patterns for values
                        for v in values:
                            # Basic pattern: value itself
                            trusted_patterns.append({"label": label, "pattern": v.lower(), "id": v})

                        # LLM Enrichment
                        if client and values and len(values) <= detector.threshold * 2:
                            logger.info(f"Enriching {label} with LLM...")
                            synonyms = await enrich_values_with_llm(
                                client, label, values, run_id=run_id
                            )
                            untrusted_patterns.extend(synonyms)

        # Validation Stage
        validator = PatternValidator()

        # 1. Validate Trusted (Allow Short)
        logger.info(f"Validating {len(trusted_patterns)} trusted patterns...")
        valid_trusted, failures_t = validator.validate_batch(trusted_patterns, allow_short=True)
        if failures_t:
            logger.warning(f"Dropped {len(failures_t)} trusted patterns:")
            for f in failures_t:
                logger.warning(
                    f"  [{f.reason}] '{f.raw_pattern}' -> '{f.sanitized_pattern}' : {f.details}"
                )

        # 2. Validate Untrusted (Reject Short, Check Overlaps against Trusted)
        logger.info(f"Validating {len(untrusted_patterns)} untrusted patterns...")
        valid_untrusted, failures_u = validator.validate_batch(
            untrusted_patterns, existing_patterns=valid_trusted, allow_short=False
        )
        if failures_u:
            logger.warning(f"Dropped {len(failures_u)} untrusted patterns:")
            for f in failures_u:
                logger.warning(
                    f"  [{f.reason}] '{f.raw_pattern}' -> '{f.sanitized_pattern}' : {f.details}"
                )

        patterns = valid_trusted + valid_untrusted
        logger.info(f"Retained {len(patterns)} valid patterns.")

    except Exception as e:
        logger.error(f"Error in pattern generation: {e}")
        raise e

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

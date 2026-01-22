#!/usr/bin/env python3
"""
Seed Enrichment Script.

This script:
1. Loads validation-ready seeds from database/seeds/by_table and database/seeds/complex.
2. Executes every query against the running DB.
3. Captures the result set and row count.
4. Enriches the seed object with metadata (id, difficulty, tables_used).
5. Writes the enriched data back to the JSON file in the target schema format.
"""

import asyncio
import datetime
import decimal
import json
import re
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv  # noqa: E402

from mcp_server.db import Database  # noqa: E402

load_dotenv()

# Configuration
TENANT_ID = 1  # Default tenant for Golden Dataset
STARTING_ID = 1000  # Start IDs here to avoid conflict with legacy seeds

# Regex to find table names (simple heuristic, can be improved)
TABLE_REGEX = re.compile(
    r"\b(actor|address|category|city|country|customer|film|film_actor|"
    r"film_category|inventory|language|payment|rental|staff|store)\b",
    re.IGNORECASE,
)


def get_difficulty(file_path: Path) -> str:
    """Determine difficulty based on folder or filename."""
    if "complex" in str(file_path):
        if "expert" in str(file_path) or "window" in str(file_path):
            return "expert"
        return "hard"
    return "easy"  # by_table defaults to easy


def get_category(file_path: Path) -> str:
    """Determine category based on filename."""
    return file_path.stem


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and decimal objects."""

    def default(self, obj):
        """Encode datetime and decimal objects."""
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


async def execute_and_heal(conn, sql: str, question: str) -> tuple[list[dict], str]:
    """Execute SQL and attempt to heal if 0 rows returned due to casing."""
    try:
        rows = await conn.fetch(sql)
        result_rows = [dict(row) for row in rows]

        # Self-healing: if 0 rows, try uppercasing string literals (simple heuristic)
        if not result_rows and "'" in sql:
            # Regex to find 'Content' and uppercase it -> 'CONTENT'
            def upper_repl(match):
                return match.group(0).upper()

            healed_sql = re.sub(r"'[^']*'", upper_repl, sql)
            if healed_sql != sql:
                print(f"     0 rows. Trying healed SQL: {healed_sql}")
                try:
                    # Create a savepoint for the trial to avoid aborting tx on error
                    async with conn.transaction():
                        healed_rows = await conn.fetch(healed_sql)
                        if healed_rows:
                            print(f"     ✅ Healed! {len(healed_rows)} rows found.")
                            return [dict(r) for r in healed_rows], healed_sql
                except Exception:
                    # Ignore healing errors
                    pass

        return result_rows, sql

    except Exception as e:
        print(f"  ❌ execution failed for: {question}")
        print(f"     SQL: {sql}")
        print(f"     Error: {e}")
        return [], sql


# Tenant Configuration for specific datasets
TENANT_MAP = {
    "payment": 25,
    "rental": 1,
    "window_functions": 25,
    "time_series": 25,
    # "cte": 1, # default
}


async def enrich_file(file_path: Path, current_id: int) -> int:
    """Enrich a single JSON file with its own DB connection/transaction."""
    print(f"Processing {file_path.name}...")

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"  ❌ Error loading file: {e}")
        return current_id

    # Handle various formats (list vs dict) and extract queries
    queries = []
    metadata = {}

    if isinstance(data, list):
        queries = data
    elif isinstance(data, dict):
        queries = data.get("queries", [])
        metadata = data.get("metadata", {})
    else:
        print(f"  ❌ Invalid format in {file_path.name}")
        return current_id

    # Determine Tenant ID
    # Priority: TENANT_MAP > Metadata > Default
    tenant_id = TENANT_MAP.get(file_path.stem)
    if tenant_id is None:
        tenant_id = metadata.get("tenant_id", TENANT_ID)

    enriched_queries = []

    # Get a fresh connection for this file to ensure isolation
    async with Database.get_connection(tenant_id=tenant_id) as conn:
        for item in queries:
            sql = item.get("sql_query") or item.get("query")
            question = item.get("question")

            if not sql:
                continue

            # Execute & Heal
            result_rows, final_sql = await execute_and_heal(conn, sql, question)

            # Metadata extraction
            tables_used = sorted(
                list(set(m.group(1).lower() for m in TABLE_REGEX.finditer(final_sql)))
            )

            # Construct Enriched Object
            new_item = {
                "id": current_id,
                "question": question,
                "query": final_sql,
                "expected_result": result_rows,
                "expected_row_count": len(result_rows),
                "category": item.get("category") or get_category(file_path),
                "difficulty": item.get("difficulty") or get_difficulty(file_path),
                "tables_used": tables_used,
            }

            enriched_queries.append(new_item)
            current_id += 1

            if len(result_rows) == 0:
                print(f"  ⚠️  Zero rows returned for: {final_sql[:50]}... (Tenant {tenant_id})")

    # Construct Final Output Schema
    final_output = {
        "metadata": {
            "version": "1.0",
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "tenant_id": tenant_id,
            "description": f"Enriched seeds for {file_path.stem}",
        },
        "queries": enriched_queries,
    }

    # Write back
    with open(file_path, "w") as f:
        json.dump(final_output, f, indent=2, cls=DateTimeEncoder)

    print(f"  ✅ Enriched {len(enriched_queries)} queries in {file_path.name} (Tenant {tenant_id})")
    return current_id


async def main():
    """Execute main enrichment process."""
    # Setup paths
    if Path("/app/queries").exists():
        base_path = Path("/app/queries")
    else:
        base_path = Path(__file__).parent.parent.parent / "database" / "queries"

    if not base_path.exists():
        print(f"Error: Queries directory not found at {base_path}")
        sys.exit(1)

    # Initialize Pool
    await Database.init()

    current_id = STARTING_ID

    try:
        # Process by_table
        by_table_dir = base_path / "by_table"
        if by_table_dir.exists():
            for f in sorted(by_table_dir.glob("*.json")):
                current_id = await enrich_file(f, current_id)

        # Process complex
        complex_dir = base_path / "complex"
        if complex_dir.exists():
            for f in sorted(complex_dir.glob("*.json")):
                current_id = await enrich_file(f, current_id)

        # Process joins
        joins_dir = base_path / "joins"
        if joins_dir.exists():
            for f in sorted(joins_dir.glob("*.json")):
                current_id = await enrich_file(f, current_id)

    finally:
        await Database.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

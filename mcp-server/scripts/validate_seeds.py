#!/usr/bin/env python3
"""
Seed Validation Script.

This script loads all seed queries from the database/seeds directory
and verifies them against the running PostgreSQL database using EXPLAIN.
This ensures that all seed queries are syntactically correct and bind to the current schema.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path so we can import mcp_server modules
sys.path.append(str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv  # noqa: E402
from mcp_server.db import Database  # noqa: E402
from mcp_server.seeding.loader import load_from_directory  # noqa: E402

load_dotenv()


async def validate_query(conn, query: str) -> str | None:
    """Run EXPLAIN on the query to check validity without executing it.

    Returns:
        None if valid, error message string if invalid.
    """
    try:
        # We use EXPLAIN to validate the query plan without running it.
        # This catches syntax errors and schema mismatches (e.g. invalid table/column).
        await conn.fetch(f"EXPLAIN {query}")
        return None
    except Exception as e:
        return str(e)


async def main():
    """Execute main validation."""
    # 1. Setup paths
    # Check for Docker path first
    if Path("/app/queries").exists():
        base_path = Path("/app/queries")
    else:
        # Local path fallback
        base_path = Path(__file__).parent.parent.parent / "database" / "queries"

    if not base_path.exists():
        print(f"Error: Seeds directory not found at {base_path}")
        sys.exit(1)

    # 2. Load all queries
    print(f"Loading seeds from {base_path}...")
    queries = load_from_directory(base_path)
    if not queries:
        print("No queries found.")
        sys.exit(0)

    print(f"Found {len(queries)} queries to validate.")

    # 3. Connect to DB
    await Database.init()

    validation_passed = True
    errors = []

    try:
        async with Database.get_connection() as conn:
            print("Connected to database. Starting validation...\n")

            for i, item in enumerate(queries):
                sql = item.get("sql_query") or item.get("query")
                question = item.get("question", "Unknown Question")

                if not sql:
                    print(f"SKIP item {i}: No SQL found.")
                    continue

                error = await validate_query(conn, sql)

                if error:
                    validation_passed = False
                    print(f"❌ FAIL: {question}")
                    print(f"   SQL: {sql}")
                    print(f"   Error: {error}\n")
                    errors.append({"question": question, "sql": sql, "error": error})
                else:
                    # Optional: Print success dot for progress
                    print(".", end="", flush=True)

    finally:
        await Database.close()

    print("\n\n" + "=" * 50)
    if validation_passed:
        print(f"✅ SUCCESS: All {len(queries)} queries verified successfully.")
        sys.exit(0)
    else:
        print(f"❌ FAILED: {len(errors)} queries failed validation.")
        sys.exit(1)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())

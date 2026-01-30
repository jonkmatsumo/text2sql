#!/usr/bin/env python3
"""Control-plane database migration runner.

This script runs SQL migrations against the control-plane database.
It should be run once before starting services, not during startup.

Usage:
    # Direct
    python scripts/migrations/migrate.py

    # Via docker compose
    docker compose run --rm mcp-server python /app/scripts/migrations/migrate.py

Environment variables:
    CONTROL_DB_HOST: Database host (required)
    CONTROL_DB_PORT: Database port (default: 5432)
    CONTROL_DB_NAME: Database name (default: agent_control)
    CONTROL_DB_USER: Database user (default: postgres)
    CONTROL_DB_PASSWORD: Database password (default: control_password)
"""

import asyncio
import os
import sys
from pathlib import Path

import asyncpg


def get_db_config() -> dict:
    """Get database configuration from environment."""
    host = os.getenv("CONTROL_DB_HOST")
    if not host:
        print("ERROR: CONTROL_DB_HOST environment variable is required")
        sys.exit(1)

    return {
        "host": host,
        "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
        "database": os.getenv("CONTROL_DB_NAME", "agent_control"),
        "user": os.getenv("CONTROL_DB_USER", "postgres"),
        "password": os.getenv("CONTROL_DB_PASSWORD", "control_password"),
    }


async def run_migration(conn: asyncpg.Connection, migration_path: Path) -> bool:
    """Run a single migration file.

    Args:
        conn: Database connection.
        migration_path: Path to the SQL migration file.

    Returns:
        True if migration was applied, False if already applied.
    """
    migration_name = migration_path.stem
    print(f"  Checking migration: {migration_name}")

    # Check if migration is already applied
    try:
        result = await conn.fetchval("SELECT 1 FROM _migrations WHERE name = $1", migration_name)
        if result:
            print(f"  [SKIP] {migration_name} already applied")
            return False
    except asyncpg.UndefinedTableError:
        # _migrations table doesn't exist yet, will be created by first migration
        pass

    # Read and execute migration
    sql = migration_path.read_text()
    print(f"  [RUN] {migration_name}")

    await conn.execute(sql)
    print(f"  [OK] {migration_name}")
    return True


async def run_all_migrations():
    """Run all pending migrations."""
    config = get_db_config()
    migrations_dir = Path(__file__).parent

    print(f"Connecting to {config['user']}@{config['host']}:{config['port']}/{config['database']}")

    try:
        conn = await asyncpg.connect(**config)
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Find all SQL migration files
        migration_files = sorted(migrations_dir.glob("*.sql"))
        if not migration_files:
            print("No migration files found")
            return

        print(f"Found {len(migration_files)} migration file(s)")
        applied = 0

        for migration_path in migration_files:
            if await run_migration(conn, migration_path):
                applied += 1

        if applied:
            print(f"Applied {applied} migration(s)")
        else:
            print("All migrations already applied")

    finally:
        await conn.close()


def main():
    """Entry point."""
    print("Control-plane database migration runner")
    print("=" * 40)
    asyncio.run(run_all_migrations())
    print("=" * 40)
    print("Done")


if __name__ == "__main__":
    main()

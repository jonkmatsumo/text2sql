"""Query-target database startup validation.

Validates the query-target database structure at startup to ensure
the system has the required schema for operation.
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from common.config.env import get_env_bool

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Validation result with structured data."""

    db_reachable: bool = True
    table_count: int = 0
    column_count: int = 0
    fk_count: int = 0
    queries_present: bool = False
    tables_json_present: bool = False
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


async def validate_query_target(conn) -> ValidationResult:
    """Validate query-target database structure.

    Args:
        conn: Active database connection

    Returns:
        ValidationResult with inspection data
    """
    result = ValidationResult()

    try:
        # Count tables in public schema
        tables = await conn.fetch(
            """
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """
        )
        result.table_count = tables[0]["cnt"]

        # Count columns
        columns = await conn.fetch(
            """
            SELECT COUNT(*) as cnt
            FROM information_schema.columns
            WHERE table_schema = 'public'
        """
        )
        result.column_count = columns[0]["cnt"]

        # Count foreign keys
        fks = await conn.fetch(
            """
            SELECT COUNT(*) as cnt
            FROM information_schema.table_constraints
            WHERE constraint_type = 'FOREIGN KEY' AND table_schema = 'public'
        """
        )
        result.fk_count = fks[0]["cnt"]

    except Exception as e:
        result.db_reachable = False
        result.errors.append(f"Database query failed: {e}")

    return result


def check_seed_artifacts(base_path: Path, result: ValidationResult) -> None:
    """Check for presence of optional seed artifacts.

    Args:
        base_path: Path to seed data directory
        result: ValidationResult to update with findings
    """
    queries_path = base_path / "queries"
    tables_json_path = base_path / "tables.json"

    if queries_path.exists() and any(queries_path.glob("*.json")):
        result.queries_present = True
    else:
        result.warnings.append(
            f"No query JSON files found in {queries_path}. " "Few-shot examples will not be seeded."
        )

    if tables_json_path.exists():
        result.tables_json_present = True
    else:
        result.warnings.append(
            f"No tables.json found at {tables_json_path}. " "Table summaries will not be seeded."
        )


def log_validation_summary(result: ValidationResult) -> None:
    """Log structured validation summary."""
    if not result.db_reachable:
        logger.error("❌ Query-Target Validation FAILED: Database unreachable")
        for error in result.errors:
            logger.error(f"   {error}")
        return

    if result.table_count == 0:
        logger.error("❌ Query-Target Validation FAILED: No tables found in public schema")
        return

    logger.info("─" * 50)
    logger.info("📊 Query-Target Database Contract Summary")
    logger.info("─" * 50)
    logger.info(f"  Tables:           {result.table_count}")
    logger.info(f"  Columns:          {result.column_count}")
    logger.info(f"  Foreign Keys:     {result.fk_count}")

    if result.queries_present:
        logger.info("  Queries:          ✓ Present")
    if result.tables_json_present:
        logger.info("  Table Summaries:  ✓ Present")

    if result.warnings:
        logger.info("  ⚠ Warnings:")
        for warning in result.warnings:
            logger.warning(f"    - {warning}")
    else:
        logger.info("  ✓ Contract satisfied")
    logger.info("─" * 50)


async def run_startup_validation(
    conn, base_path: Optional[Path] = None, fail_fast: Optional[bool] = None
) -> bool:
    """Run validation and optionally fail fast.

    Args:
        conn: Database connection
        base_path: Optional path to seed data directory for artifact checks
        fail_fast: If True, exit on critical failures.
                   Defaults to SEEDING_FAIL_FAST env var.

    Returns:
        True if validation passed, False otherwise
    """
    if fail_fast is None:
        fail_fast = get_env_bool("SEEDING_FAIL_FAST", False)

    result = await validate_query_target(conn)

    # Check seed artifacts if path provided
    if base_path:
        check_seed_artifacts(base_path, result)

    log_validation_summary(result)

    # Critical failures
    if not result.db_reachable or result.table_count == 0:
        if fail_fast:
            logger.error("Fail-fast enabled. Exiting due to validation failure.")
            sys.exit(1)
        return False

    return True

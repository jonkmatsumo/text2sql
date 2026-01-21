"""Main orchestration for synthetic data generation."""

from __future__ import annotations

import logging

import pandas as pd
from text2sql_synth import generators, schema
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext, TimeWindowConfig

logger = logging.getLogger(__name__)


def generate_all(cfg: SynthConfig) -> tuple[GenerationContext, dict[str, pd.DataFrame]]:
    """Generate all tables in the correct dependency order.

    Args:
        cfg: Configuration for generation.

    Returns:
        A tuple of (GenerationContext, dict of table_name -> DataFrame).
    """
    return generate_tables(cfg, only=None)


def generate_tables(
    cfg: SynthConfig, only: list[str] | None = None
) -> tuple[GenerationContext, dict[str, pd.DataFrame]]:
    """Generate specified tables and their dependencies in correct order.

    Args:
        cfg: Configuration for generation.
        only: List of tables to generate. If None, generates all tables.
              Dependencies of requested tables will be automatically included.

    Returns:
        A tuple of (GenerationContext, dict of table_name -> DataFrame).
    """
    time_window = TimeWindowConfig(
        start_date=cfg.time_window.start_date,
        end_date=cfg.time_window.end_date,
    )

    ctx = GenerationContext(
        seed=cfg.seed,
        schema_definition="text2sql_v1",
        time_window=time_window,
    )

    # Determine which tables need to be generated
    tables_to_generate = _resolve_dependencies(only)

    logger.info(f"Starting synthetic data generation for {len(tables_to_generate)} tables...")

    # Generate tables in the predefined order to satisfy dependencies
    for table_name in schema.GENERATION_ORDER:
        if table_name not in tables_to_generate:
            continue

        generator_name = f"generate_{table_name}"
        generator_func = getattr(generators, generator_name, None)

        if not generator_func:
            logger.warning(
                f"No generator found for table '{table_name}' (expected '{generator_name}')"
            )
            continue

        logger.info(f"Generating {table_name}...")
        df = generator_func(ctx, cfg)

        # Validation
        _validate_table(table_name, df)

        # Ensure it's registered (though generators should do this)
        if table_name not in ctx.tables:
            ctx.register_table(table_name, df)

    logger.info(f"Generation complete. Total tables generated: {len(ctx.tables)}")
    return ctx, ctx.tables


def _resolve_dependencies(only: list[str] | None) -> set[str]:
    """Resolve the full set of tables required, including recursive dependencies.

    Args:
        only: Initial list of requested tables.

    Returns:
        Complete set of required table names.
    """
    if only is None:
        return set(schema.GENERATION_ORDER)

    required = set()
    to_visit = list(only)

    while to_visit:
        table = to_visit.pop()
        if table in required:
            continue

        if table not in schema.DEPENDENCIES:
            # We allow it even if not in dependencies mapping, just won't have dependencies
            logger.debug(f"Table '{table}' not found in dependency map.")
            required.add(table)
            continue

        required.add(table)
        # Add dependencies to visit
        for dep in schema.DEPENDENCIES.get(table, []):
            if dep not in required:
                to_visit.append(dep)

    return required


def _validate_table(table_name: str, df: pd.DataFrame) -> None:
    """Validate that the generated DataFrame has the expected columns.

    Args:
        table_name: Name of the table.
        df: Generated DataFrame.

    Raises:
        ValueError: If columns are missing.
    """
    expected = schema.EXPECTED_COLUMNS.get(table_name)
    if expected is None:
        logger.debug(f"No column validation defined for table '{table_name}'")
        return

    actual = set(df.columns)
    missing = set(expected) - actual

    if missing:
        error_msg = f"Table '{table_name}' is missing expected columns: {missing}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.debug(f"Validated columns for {table_name}")

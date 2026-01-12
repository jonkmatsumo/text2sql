"""Main orchestration for synthetic data generation."""

from __future__ import annotations

import logging
from typing import Any

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext, TimeWindowConfig
from text2sql_synth.generators import (
    generate_bridge_customer_address,
    generate_dim_account,
    generate_dim_address,
    generate_dim_counterparty,
    generate_dim_customer,
    generate_dim_customer_scd2,
    generate_dim_institution,
    generate_dim_merchant,
    generate_dim_time,
    generate_fact_dispute,
    generate_fact_payment,
    generate_fact_refund,
    generate_fact_transaction,
)

logger = logging.getLogger(__name__)


def generate_all(cfg: SynthConfig) -> GenerationContext:
    """Generate all tables in the correct dependency order.

    Args:
        cfg: Configuration for generation.

    Returns:
        GenerationContext containing all generated DataFrames.
    """
    time_window = TimeWindowConfig(
        start_date=cfg.time_window.start_date,
        end_date=cfg.time_window.end_date,
    )
    
    ctx = GenerationContext(
        seed=cfg.seed,
        schema_definition="text2sql_v1",  # Could be configurable if needed
        time_window=time_window,
    )

    logger.info("Starting synthetic data generation...")

    # 1. Time dimension (no dependencies)
    logger.info("Generating dim_time...")
    generate_dim_time(ctx, cfg)

    # 2. Basic dimensions
    logger.info("Generating dim_institution...")
    generate_dim_institution(ctx, cfg)
    
    logger.info("Generating dim_address...")
    generate_dim_address(ctx, cfg)

    # 3. Customer & Merchant (depends on address)
    logger.info("Generating dim_customer...")
    generate_dim_customer(ctx, cfg)
    
    logger.info("Generating dim_merchant...")
    generate_dim_merchant(ctx, cfg)

    # 4. Account (depends on customer, institution)
    logger.info("Generating dim_account...")
    generate_dim_account(ctx, cfg)

    # 5. Bridges and related dimensions
    logger.info("Generating bridge_customer_address...")
    generate_bridge_customer_address(ctx, cfg)
    
    logger.info("Generating dim_counterparty...")
    generate_dim_counterparty(ctx, cfg)
    
    logger.info("Generating dim_customer_scd2...")
    generate_dim_customer_scd2(ctx, cfg)

    # 6. Primary facts (depend on basic dimensions)
    logger.info("Generating fact_transaction...")
    generate_fact_transaction(ctx, cfg)

    # 7. Secondary facts (depend on primary facts)
    logger.info("Generating fact_payment...")
    generate_fact_payment(ctx, cfg)
    
    logger.info("Generating fact_refund...")
    generate_fact_refund(ctx, cfg)
    
    logger.info("Generating fact_dispute...")
    generate_fact_dispute(ctx, cfg)

    logger.info("Generation complete. Total tables: %d", len(ctx.tables))
    return ctx

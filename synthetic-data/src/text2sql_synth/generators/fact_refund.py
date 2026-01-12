"""Generator for fact_refund fact table.

Refund records linked to transactions with refund amounts and reasons.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "fact_refund"

# Refund reasons
REFUND_REASONS = [
    "customer_request",
    "product_defective",
    "wrong_item",
    "item_not_received",
    "duplicate_charge",
    "service_unsatisfactory",
    "price_adjustment",
    "order_cancelled",
]
REFUND_REASON_WEIGHTS = [0.25, 0.15, 0.12, 0.12, 0.10, 0.10, 0.08, 0.08]

# Refund statuses
REFUND_STATUSES = ["pending", "approved", "processed", "rejected"]

# Risk tier multipliers for refund rate
RISK_REFUND_MULTIPLIERS = {
    "low": 0.8,
    "medium": 1.0,
    "high": 1.5,
    "critical": 2.5,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the fact_refund fact table.

    Creates refund records with:
    - refund_id: Unique identifier
    - transaction_id: FK to fact_transaction
    - refund_amount: Amount refunded (may be partial)
    - refund_reason: Reason for refund
    - refund_status: pending, approved, processed, rejected
    - refund_requested_ts: When refund was requested
    - refund_processed_ts: When refund was processed (nullable)
    - is_partial: Whether refund is partial
    - refund_method: original_payment, store_credit, check
    - processing_fee_refunded: Whether processing fee was refunded

    Refund rate is based on config.rates.refund_rate, modified by risk tier.
    Only approved transactions can have refunds.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with rates.

    Returns:
        DataFrame with refund fact data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get transaction data
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None or len(transaction_df) == 0:
        raise ValueError("fact_transaction must be generated before fact_refund")

    # Only approved transactions can have refunds
    approved_txns = transaction_df[transaction_df["status"] == "approved"]

    base_refund_rate = cfg.rates.refund_rate

    rows = []

    for _, txn_row in approved_txns.iterrows():
        # Apply risk-correlated refund rate
        risk_tier = txn_row["risk_tier"]
        risk_multiplier = RISK_REFUND_MULTIPLIERS.get(risk_tier, 1.0)
        effective_refund_rate = min(base_refund_rate * risk_multiplier, 0.3)

        # Determine if this transaction gets a refund
        if rng.random() >= effective_refund_rate:
            continue

        refund_id = ctx.stable_id("ref")

        # Refund amount (80% full, 20% partial)
        gross_amount = txn_row["gross_amount"]
        is_partial = rng.random() < 0.2
        if is_partial:
            # Partial refund: 20-80% of original
            refund_pct = 0.2 + rng.random() * 0.6
            refund_amount = round(gross_amount * refund_pct, 2)
        else:
            refund_amount = gross_amount

        # Refund reason
        refund_reason = ctx.sample_categorical(rng, REFUND_REASONS, weights=REFUND_REASON_WEIGHTS)

        # Refund timing (1-30 days after transaction)
        days_until_request = int(rng.integers(1, 31))
        refund_requested_ts = txn_row["transaction_ts"] + timedelta(days=days_until_request)

        # Refund status
        status_roll = rng.random()
        if status_roll < 0.85:
            refund_status = "processed"
            # Processing takes 1-7 days
            processing_days = int(rng.integers(1, 8))
            refund_processed_ts = refund_requested_ts + timedelta(days=processing_days)
        elif status_roll < 0.95:
            refund_status = "approved"
            refund_processed_ts = None
        elif status_roll < 0.98:
            refund_status = "pending"
            refund_processed_ts = None
        else:
            refund_status = "rejected"
            refund_processed_ts = None

        # Refund method
        refund_method = ctx.sample_categorical(
            rng,
            ["original_payment", "store_credit", "check"],
            weights=[0.85, 0.10, 0.05],
        )

        # Processing fee refund (usually not refunded)
        processing_fee_refunded = rng.random() < 0.1

        row = {
            "refund_id": refund_id,
            "transaction_id": txn_row["transaction_id"],
            "refund_amount": refund_amount,
            "refund_reason": refund_reason,
            "refund_status": refund_status,
            "refund_requested_ts": refund_requested_ts,
            "refund_processed_ts": refund_processed_ts,
            "is_partial": is_partial,
            "refund_method": refund_method,
            "processing_fee_refunded": processing_fee_refunded,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

"""Generator for fact_dispute fact table.

Dispute records linked to transactions with dispute status tracking.
Strong risk correlation - higher risk tiers have higher dispute rates.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "fact_dispute"

# Dispute reasons
DISPUTE_REASONS = [
    "fraud",
    "unauthorized",
    "product_not_received",
    "product_not_as_described",
    "duplicate_charge",
    "cancelled_recurring",
    "credit_not_processed",
    "incorrect_amount",
]
DISPUTE_REASON_WEIGHTS = [0.20, 0.18, 0.15, 0.12, 0.10, 0.10, 0.08, 0.07]

# Dispute statuses
DISPUTE_STATUSES = [
    "opened",
    "under_review",
    "merchant_response",
    "resolved_customer",
    "resolved_merchant",
    "chargeback",
    "arbitration",
]

# Resolution outcomes
RESOLUTION_OUTCOMES = [
    "customer_won",
    "merchant_won",
    "partial_credit",
    "withdrawn",
    "expired",
]

# Risk tier multipliers for dispute rate - STRONG correlation
RISK_DISPUTE_MULTIPLIERS = {
    "low": 0.3,
    "medium": 1.0,
    "high": 3.0,
    "critical": 8.0,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the fact_dispute fact table.

    Creates dispute records with:
    - dispute_id: Unique identifier
    - transaction_id: FK to fact_transaction
    - dispute_reason: Reason for dispute
    - dispute_status: Current status
    - dispute_amount: Amount in dispute
    - dispute_opened_ts: When dispute was opened
    - dispute_resolved_ts: When dispute was resolved (nullable)
    - resolution_outcome: Final resolution (nullable if not resolved)
    - is_chargeback: Whether dispute resulted in chargeback
    - merchant_responded: Whether merchant responded
    - evidence_submitted: Whether customer submitted evidence
    - days_to_resolution: Days from open to resolution (nullable)

    Dispute rate is based on config.rates.dispute_rate with STRONG
    risk tier correlation - high risk accounts dispute much more often.

    Disputes are distinct from refunds:
    - Refunds are merchant-initiated returns
    - Disputes are customer-initiated complaints to card issuer

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with rates.

    Returns:
        DataFrame with dispute fact data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get transaction data
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None or len(transaction_df) == 0:
        raise ValueError("fact_transaction must be generated before fact_dispute")

    # Only approved transactions can have disputes (customer paid, then disputes)
    approved_txns = transaction_df[transaction_df["status"] == "approved"]

    base_dispute_rate = cfg.rates.dispute_rate
    chargeback_rate = cfg.rates.chargeback_rate

    rows = []

    for _, txn_row in approved_txns.iterrows():
        # Apply STRONG risk-correlated dispute rate
        risk_tier = txn_row["risk_tier"]
        risk_multiplier = RISK_DISPUTE_MULTIPLIERS.get(risk_tier, 1.0)
        effective_dispute_rate = min(base_dispute_rate * risk_multiplier, 0.25)

        # Determine if this transaction gets a dispute
        if rng.random() >= effective_dispute_rate:
            continue

        dispute_id = ctx.stable_id("disp")

        # Dispute amount (usually full amount, sometimes partial)
        gross_amount = txn_row["gross_amount"]
        if rng.random() < 0.9:
            dispute_amount = gross_amount
        else:
            dispute_amount = round(gross_amount * (0.5 + rng.random() * 0.5), 2)

        # Dispute reason - fraud/unauthorized more common for high risk
        if risk_tier in ["high", "critical"] and rng.random() < 0.5:
            dispute_reason = ctx.sample_categorical(
                rng, ["fraud", "unauthorized"], weights=[0.6, 0.4]
            )
        else:
            dispute_reason = ctx.sample_categorical(
                rng, DISPUTE_REASONS, weights=DISPUTE_REASON_WEIGHTS
            )

        # Dispute timing (3-60 days after transaction for most card disputes)
        days_until_dispute = int(rng.integers(3, 61))
        dispute_opened_ts = txn_row["transaction_ts"] + timedelta(days=days_until_dispute)

        # Dispute status and resolution
        status_roll = rng.random()
        if status_roll < 0.70:
            # Resolved disputes
            if rng.random() < chargeback_rate * 10:  # Relative to disputes
                dispute_status = "chargeback"
                is_chargeback = True
            else:
                dispute_status = ctx.sample_categorical(
                    rng,
                    ["resolved_customer", "resolved_merchant"],
                    weights=[0.55, 0.45],
                )
                is_chargeback = False

            # Resolution takes 15-90 days
            days_to_resolution = int(rng.integers(15, 91))
            dispute_resolved_ts = dispute_opened_ts + timedelta(days=days_to_resolution)

            # Resolution outcome
            if is_chargeback:
                resolution_outcome = "customer_won"
            elif dispute_status == "resolved_customer":
                resolution_outcome = ctx.sample_categorical(
                    rng,
                    ["customer_won", "partial_credit"],
                    weights=[0.7, 0.3],
                )
            else:
                resolution_outcome = ctx.sample_categorical(
                    rng,
                    ["merchant_won", "withdrawn", "expired"],
                    weights=[0.6, 0.3, 0.1],
                )
        else:
            # Open/in-progress disputes
            dispute_status = ctx.sample_categorical(
                rng,
                ["opened", "under_review", "merchant_response", "arbitration"],
                weights=[0.3, 0.4, 0.2, 0.1],
            )
            dispute_resolved_ts = None
            resolution_outcome = None
            days_to_resolution = None
            is_chargeback = False

        # Merchant response
        merchant_responded = rng.random() < 0.75

        # Customer evidence
        evidence_submitted = rng.random() < 0.60

        row = {
            "dispute_id": dispute_id,
            "transaction_id": txn_row["transaction_id"],
            "dispute_reason": dispute_reason,
            "dispute_status": dispute_status,
            "dispute_amount": dispute_amount,
            "dispute_opened_ts": dispute_opened_ts,
            "dispute_resolved_ts": dispute_resolved_ts,
            "resolution_outcome": resolution_outcome,
            "is_chargeback": is_chargeback,
            "merchant_responded": merchant_responded,
            "evidence_submitted": evidence_submitted,
            "days_to_resolution": days_to_resolution,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

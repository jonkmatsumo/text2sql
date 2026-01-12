"""Generator for fact_payment fact table.

Payment details linked to transactions including payment method,
settlement status, and ACH return handling.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "fact_payment"

# Payment methods
PAYMENT_METHODS = ["credit_card", "debit_card", "ach", "wire", "digital_wallet"]
PAYMENT_METHOD_WEIGHTS = [0.35, 0.30, 0.15, 0.05, 0.15]

# Card networks
CARD_NETWORKS = ["visa", "mastercard", "amex", "discover"]
CARD_NETWORK_WEIGHTS = [0.45, 0.30, 0.15, 0.10]

# Settlement statuses
SETTLEMENT_STATUSES = ["pending", "settled", "failed", "reversed"]

# ACH return codes
ACH_RETURN_CODES = [
    ("R01", "Insufficient Funds"),
    ("R02", "Account Closed"),
    ("R03", "No Account/Unable to Locate"),
    ("R04", "Invalid Account Number"),
    ("R10", "Customer Advises Not Authorized"),
    ("R29", "Corporate Customer Advises Not Authorized"),
]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the fact_payment fact table.

    Creates payment records with:
    - payment_id: Unique identifier
    - transaction_id: FK to fact_transaction
    - payment_method: credit_card, debit_card, ach, wire, digital_wallet
    - card_network: visa, mastercard, amex, discover (nullable for non-card)
    - card_last_four: Last 4 digits of card (nullable)
    - auth_code: Authorization code
    - settlement_status: pending, settled, failed, reversed
    - settlement_ts: When settlement completed (nullable)
    - ach_return_code: ACH return code (nullable)
    - ach_return_reason: ACH return reason (nullable)
    - processing_fee: Fee charged for processing
    - interchange_fee: Interchange fee

    Every approved transaction gets a payment record.
    Declined transactions may or may not have payment attempts.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with rates.

    Returns:
        DataFrame with payment fact data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get transaction data
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None or len(transaction_df) == 0:
        raise ValueError("fact_transaction must be generated before fact_payment")

    rows = []

    for _, txn_row in transaction_df.iterrows():
        # All approved transactions get payment records
        # 50% of declined also have payment attempt records
        if txn_row["status"] == "declined" and rng.random() > 0.5:
            continue

        payment_id = ctx.stable_id("pay")

        # Payment method
        payment_method = ctx.sample_categorical(
            rng, PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS
        )

        # Card-specific fields
        card_network = None
        card_last_four = None
        if payment_method in ["credit_card", "debit_card"]:
            card_network = ctx.sample_categorical(rng, CARD_NETWORKS, weights=CARD_NETWORK_WEIGHTS)
            card_last_four = f"{rng.integers(0, 9999):04d}"

        # Auth code (8 char alphanumeric)
        auth_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        auth_code = "".join(rng.choice(list(auth_chars), size=8))

        # Settlement status and timestamp
        if txn_row["status"] == "approved":
            # Most approved transactions settle
            if rng.random() < 0.98:
                settlement_status = "settled"
                # Settlement typically 1-3 days after transaction
                settlement_days = int(rng.integers(1, 4))
                settlement_ts = txn_row["transaction_ts"] + timedelta(days=settlement_days)
            else:
                settlement_status = ctx.sample_categorical(
                    rng, ["pending", "failed", "reversed"], weights=[0.5, 0.3, 0.2]
                )
                settlement_ts = None
        else:
            # Declined transactions
            settlement_status = "failed"
            settlement_ts = None

        # ACH-specific fields
        ach_return_code = None
        ach_return_reason = None
        if payment_method == "ach" and settlement_status in ["failed", "reversed"]:
            # ACH returns
            return_idx = rng.integers(0, len(ACH_RETURN_CODES))
            ach_return_code = ACH_RETURN_CODES[return_idx][0]
            ach_return_reason = ACH_RETURN_CODES[return_idx][1]

        # Fees
        gross_amount = txn_row["gross_amount"]

        # Processing fee (varies by method)
        if payment_method in ["credit_card", "debit_card"]:
            processing_fee = round(gross_amount * (0.015 + rng.random() * 0.01), 2)
        elif payment_method == "ach":
            processing_fee = round(0.25 + rng.random() * 0.25, 2)  # Flat fee
        elif payment_method == "wire":
            processing_fee = round(15.0 + rng.random() * 10.0, 2)  # Higher flat fee
        else:  # digital_wallet
            processing_fee = round(gross_amount * (0.02 + rng.random() * 0.01), 2)

        # Interchange fee (card transactions only)
        if payment_method in ["credit_card", "debit_card"]:
            interchange_fee = round(gross_amount * (0.01 + rng.random() * 0.02), 2)
        else:
            interchange_fee = 0.0

        row = {
            "payment_id": payment_id,
            "transaction_id": txn_row["transaction_id"],
            "payment_method": payment_method,
            "card_network": card_network,
            "card_last_four": card_last_four,
            "auth_code": auth_code,
            "settlement_status": settlement_status,
            "settlement_ts": settlement_ts,
            "ach_return_code": ach_return_code,
            "ach_return_reason": ach_return_reason,
            "processing_fee": processing_fee,
            "interchange_fee": interchange_fee,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

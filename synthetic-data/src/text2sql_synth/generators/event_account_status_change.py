"""Generator for event_account_status_change event table.

Account status change events including freezes tied to fraud/dispute patterns.
CRITICAL for "ever frozen" style queries.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "event_account_status_change"

# Status change types
STATUS_TYPES = ["active", "inactive", "suspended", "frozen", "closed", "under_review"]

# Reasons for status changes
FREEZE_REASONS = [
    "fraud_detected",
    "dispute_cluster",
    "suspicious_activity",
    "velocity_breach",
    "compliance_review",
    "customer_request",
]

SUSPENSION_REASONS = [
    "payment_delinquent",
    "terms_violation",
    "security_concern",
    "pending_verification",
]

# Risk tier freeze rate multipliers - HIGH correlation
RISK_FREEZE_MULTIPLIERS = {
    "low": 0.2,
    "medium": 1.0,
    "high": 4.0,
    "critical": 10.0,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the event_account_status_change event table.

    Creates status change records with:
    - event_id: Unique identifier
    - account_id: FK to dim_account
    - customer_id: FK to dim_customer
    - previous_status: Status before change
    - new_status: Status after change
    - change_ts: When change occurred
    - change_reason: Reason for status change
    - initiated_by: system, customer, agent, compliance
    - related_dispute_id: FK to fact_dispute (nullable)
    - related_transaction_id: FK to fact_transaction (nullable)
    - notes: Additional notes (nullable)

    Freeze events are STRONGLY correlated with:
    - High-risk accounts (risk tier)
    - Accounts with disputes
    - Accounts with fraud-flagged transactions

    This enables "ever frozen" queries like:
    SELECT * FROM accounts WHERE account_id IN (
        SELECT account_id FROM event_account_status_change
        WHERE new_status = 'frozen'
    )

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with freeze rate.

    Returns:
        DataFrame with status change event data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get account data
    account_df = ctx.get_table("dim_account")
    if account_df is None or len(account_df) == 0:
        raise ValueError("dim_account must be generated before event_account_status_change")

    # Get dispute data for correlation
    dispute_df = ctx.get_table("fact_dispute")
    if dispute_df is not None and len(dispute_df) > 0:
        # Accounts with disputes
        disputes_by_account = (
            dispute_df.groupby(
                dispute_df.merge(
                    ctx.get_table("fact_transaction")[["transaction_id", "account_id"]],
                    on="transaction_id",
                )["account_id"]
            )
            .agg(
                {
                    "dispute_id": "count",
                    "dispute_opened_ts": "max",
                }
            )
            .to_dict()
        )
    else:
        disputes_by_account = {"dispute_id": {}, "dispute_opened_ts": {}}

    # Get fraud-flagged transactions
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is not None:
        fraud_by_account = (
            transaction_df[transaction_df["is_fraud_flagged"]]
            .groupby("account_id")
            .agg(
                {
                    "transaction_id": ["count", "first"],
                    "transaction_ts": "max",
                }
            )
        )
        if len(fraud_by_account) > 0:
            fraud_by_account.columns = ["fraud_count", "first_fraud_txn", "last_fraud_ts"]
            fraud_accounts = fraud_by_account.to_dict("index")
        else:
            fraud_accounts = {}
    else:
        fraud_accounts = {}

    # Get time range
    time_df = ctx.get_table("dim_time")
    dates = sorted(time_df["full_date"].tolist())

    base_freeze_rate = cfg.rates.freeze_rate

    rows = []

    for _, acct_row in account_df.iterrows():
        account_id = acct_row["account_id"]
        customer_id = acct_row["customer_id"]
        risk_tier = acct_row["risk_tier"]
        opened_date = acct_row["opened_date"]

        # Base freeze rate modified by risk tier
        risk_multiplier = RISK_FREEZE_MULTIPLIERS.get(risk_tier, 1.0)
        effective_freeze_rate = base_freeze_rate * risk_multiplier

        # Additional boost if account has disputes or fraud
        has_disputes = account_id in disputes_by_account.get("dispute_id", {})
        has_fraud = account_id in fraud_accounts

        if has_disputes:
            effective_freeze_rate *= 3.0  # 3x more likely if disputes
        if has_fraud:
            effective_freeze_rate *= 5.0  # 5x more likely if fraud flagged

        # Cap at reasonable maximum
        effective_freeze_rate = min(effective_freeze_rate, 0.5)

        # Generate initial status event (account opening)
        event_id = ctx.stable_id("status")
        opening_ts = datetime.combine(opened_date, datetime.min.time()).replace(
            hour=9, minute=rng.integers(0, 60)
        )

        rows.append(
            {
                "event_id": event_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "previous_status": None,
                "new_status": "active",
                "change_ts": opening_ts,
                "change_reason": "account_opened",
                "initiated_by": "system",
                "related_dispute_id": None,
                "related_transaction_id": None,
                "notes": None,
            }
        )

        current_status = "active"

        # Determine if this account should be frozen
        if rng.random() < effective_freeze_rate:
            # Generate freeze event
            event_id = ctx.stable_id("status")

            # Freeze timing - sometime during the time window
            valid_dates = [d for d in dates if d > opened_date]
            if valid_dates:
                freeze_date = valid_dates[rng.integers(0, len(valid_dates))]
                freeze_ts = datetime.combine(freeze_date, datetime.min.time()).replace(
                    hour=rng.integers(8, 20),
                    minute=rng.integers(0, 60),
                )

                # Determine freeze reason
                if has_fraud:
                    freeze_reason = "fraud_detected"
                    related_txn = fraud_accounts.get(account_id, {}).get("first_fraud_txn")
                    related_dispute = None
                elif has_disputes:
                    freeze_reason = "dispute_cluster"
                    related_txn = None
                    # Get a dispute ID if available
                    account_disputes = (
                        dispute_df[
                            dispute_df.merge(
                                transaction_df[["transaction_id", "account_id"]],
                                on="transaction_id",
                            )["account_id"]
                            == account_id
                        ]
                        if dispute_df is not None
                        else pd.DataFrame()
                    )
                    related_dispute = (
                        account_disputes["dispute_id"].iloc[0]
                        if len(account_disputes) > 0
                        else None
                    )
                else:
                    freeze_reason = ctx.sample_categorical(
                        rng,
                        FREEZE_REASONS,
                        weights=[0.25, 0.20, 0.25, 0.15, 0.10, 0.05],
                    )
                    related_txn = None
                    related_dispute = None

                rows.append(
                    {
                        "event_id": event_id,
                        "account_id": account_id,
                        "customer_id": customer_id,
                        "previous_status": current_status,
                        "new_status": "frozen",
                        "change_ts": freeze_ts,
                        "change_reason": freeze_reason,
                        "initiated_by": (
                            "system" if freeze_reason != "customer_request" else "customer"
                        ),
                        "related_dispute_id": related_dispute,
                        "related_transaction_id": related_txn,
                        "notes": f"Automated freeze: {freeze_reason}",
                    }
                )

                current_status = "frozen"

                # Some frozen accounts get unfrozen later
                if rng.random() < 0.4:
                    unfreeze_date = freeze_date + timedelta(days=int(rng.integers(1, 30)))
                    if unfreeze_date <= dates[-1]:
                        event_id = ctx.stable_id("status")
                        unfreeze_ts = datetime.combine(unfreeze_date, datetime.min.time()).replace(
                            hour=rng.integers(8, 18),
                            minute=rng.integers(0, 60),
                        )

                        rows.append(
                            {
                                "event_id": event_id,
                                "account_id": account_id,
                                "customer_id": customer_id,
                                "previous_status": "frozen",
                                "new_status": "active",
                                "change_ts": unfreeze_ts,
                                "change_reason": "review_completed",
                                "initiated_by": "agent",
                                "related_dispute_id": None,
                                "related_transaction_id": None,
                                "notes": "Account restored after review",
                            }
                        )
                        current_status = "active"

        # Some accounts have suspension events
        if current_status == "active" and rng.random() < 0.02 * risk_multiplier:
            valid_dates = [d for d in dates if d > opened_date]
            if valid_dates:
                suspend_date = valid_dates[rng.integers(0, len(valid_dates))]
                suspend_ts = datetime.combine(suspend_date, datetime.min.time()).replace(
                    hour=rng.integers(8, 20),
                    minute=rng.integers(0, 60),
                )

                event_id = ctx.stable_id("status")
                suspend_reason = ctx.sample_categorical(rng, SUSPENSION_REASONS)

                rows.append(
                    {
                        "event_id": event_id,
                        "account_id": account_id,
                        "customer_id": customer_id,
                        "previous_status": current_status,
                        "new_status": "suspended",
                        "change_ts": suspend_ts,
                        "change_reason": suspend_reason,
                        "initiated_by": "system",
                        "related_dispute_id": None,
                        "related_transaction_id": None,
                        "notes": None,
                    }
                )

        # Handle closed accounts from dim_account
        if acct_row["account_status"] == "closed" and acct_row["closed_date"] is not None:
            event_id = ctx.stable_id("status")
            close_ts = datetime.combine(acct_row["closed_date"], datetime.min.time()).replace(
                hour=rng.integers(9, 17),
                minute=rng.integers(0, 60),
            )

            rows.append(
                {
                    "event_id": event_id,
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "previous_status": current_status,
                    "new_status": "closed",
                    "change_ts": close_ts,
                    "change_reason": "account_closed",
                    "initiated_by": ctx.sample_categorical(
                        rng, ["customer", "system"], weights=[0.7, 0.3]
                    ),
                    "related_dispute_id": None,
                    "related_transaction_id": None,
                    "notes": None,
                }
            )

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

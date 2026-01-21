"""Generator for event_account_balance_daily event table.

Daily balance snapshots for accounts tied to transaction activity.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "event_account_balance_daily"


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the event_account_balance_daily event table.

    Creates daily balance records with:
    - balance_id: Unique identifier
    - account_id: FK to dim_account
    - balance_date: Date of balance snapshot
    - opening_balance: Balance at start of day
    - closing_balance: Balance at end of day
    - daily_credits: Sum of credits (deposits, refunds)
    - daily_debits: Sum of debits (purchases, fees)
    - daily_net_change: closing_balance - opening_balance
    - transaction_count: Number of transactions
    - available_credit: For credit accounts (nullable)
    - pending_amount: Pending transactions (nullable)

    Balance changes reconcile with fact_transaction amounts.
    For medium/large scales, can sample accounts to reduce size.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration.

    Returns:
        DataFrame with daily balance data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get account data
    account_df = ctx.get_table("dim_account")
    if account_df is None or len(account_df) == 0:
        raise ValueError("dim_account must be generated before event_account_balance_daily")

    # Get transaction data
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None:
        transaction_df = pd.DataFrame()

    # Get refund data
    refund_df = ctx.get_table("fact_refund")

    # Get time range
    time_df = ctx.get_table("dim_time")
    dates = sorted(time_df["full_date"].tolist())

    # For large scales, sample accounts to keep manageable
    # small: all accounts, mvp/medium: sample
    num_accounts = len(account_df)
    num_days = len(dates)
    total_rows = num_accounts * num_days

    if total_rows > 50000:
        # Sample accounts - keep ~50% for MVP, ~20% for larger
        sample_rate = min(1.0, 50000 / total_rows)
        sampled_accounts = account_df.sample(frac=sample_rate, random_state=cfg.seed)[
            "account_id"
        ].tolist()
    else:
        sampled_accounts = account_df["account_id"].tolist()

    # Pre-compute daily transactions per account
    if len(transaction_df) > 0:
        # Add date column
        txn_df = transaction_df.copy()
        txn_df["txn_date"] = pd.to_datetime(txn_df["transaction_ts"]).dt.date

        # Aggregate by account and date
        daily_txns = (
            txn_df[txn_df["status"] == "approved"]
            .groupby(["account_id", "txn_date"])
            .agg(
                {
                    "gross_amount": "sum",
                    "fee_amount": "sum",
                    "transaction_id": "count",
                }
            )
            .reset_index()
        )
        daily_txns.columns = ["account_id", "txn_date", "total_debits", "total_fees", "txn_count"]
    else:
        daily_txns = pd.DataFrame(
            columns=["account_id", "txn_date", "total_debits", "total_fees", "txn_count"]
        )

    # Pre-compute daily refunds per account
    if refund_df is not None and len(refund_df) > 0:
        # Get account_id from transaction
        refund_with_account = refund_df.merge(
            transaction_df[["transaction_id", "account_id"]],
            on="transaction_id",
        )
        refund_with_account["refund_date"] = pd.to_datetime(
            refund_with_account["refund_processed_ts"]
        ).dt.date

        daily_refunds = (
            refund_with_account[refund_with_account["refund_status"] == "processed"]
            .groupby(["account_id", "refund_date"])
            .agg(
                {
                    "refund_amount": "sum",
                }
            )
            .reset_index()
        )
        daily_refunds.columns = ["account_id", "refund_date", "total_credits"]
    else:
        daily_refunds = pd.DataFrame(columns=["account_id", "refund_date", "total_credits"])

    # Create lookup for daily transactions
    txn_lookup: Dict[tuple, dict] = {}
    for _, row in daily_txns.iterrows():
        key = (row["account_id"], row["txn_date"])
        txn_lookup[key] = {
            "debits": row["total_debits"],
            "fees": row["total_fees"],
            "count": row["txn_count"],
        }

    # Create lookup for daily refunds
    refund_lookup: Dict[tuple, float] = {}
    for _, row in daily_refunds.iterrows():
        key = (row["account_id"], row["refund_date"])
        refund_lookup[key] = row["total_credits"]

    # Get account metadata
    account_info = account_df.set_index("account_id").to_dict("index")

    rows = []

    for account_id in sampled_accounts:
        acct = account_info.get(account_id, {})
        account_type = acct.get("account_type", "checking")
        credit_limit = acct.get("credit_limit")
        opened_date = acct.get("opened_date")
        closed_date = acct.get("closed_date")

        # Initial balance (Pareto distributed)
        if account_type == "credit":
            # Credit accounts start at 0 (no balance owed)
            opening_balance = 0.0
        else:
            # Checking/savings have positive balance
            opening_balance = round(float(ctx.sample_pareto(rng, 1.5, scale=500.0)), 2)
            opening_balance = min(opening_balance, 100000.0)

        current_balance = opening_balance

        for d in dates:
            # Skip if before account opened
            if opened_date and d < opened_date:
                continue
            # Skip if after account closed
            if closed_date and d > closed_date:
                continue

            balance_id = ctx.stable_id("bal")

            # Get daily activity
            key = (account_id, d)
            txn_data = txn_lookup.get(key, {"debits": 0.0, "fees": 0.0, "count": 0})
            credit_amount = refund_lookup.get(key, 0.0)

            daily_debits = txn_data["debits"] + txn_data["fees"]
            daily_credits = credit_amount

            # For non-credit accounts, also add random deposits
            if account_type != "credit" and rng.random() < 0.1:
                deposit = round(rng.uniform(50, 5000), 2)
                daily_credits += deposit

            opening_balance_today = current_balance
            daily_net_change = daily_credits - daily_debits
            closing_balance = round(opening_balance_today + daily_net_change, 2)

            # Available credit (for credit accounts)
            available_credit = None
            if credit_limit:
                available_credit = max(0, credit_limit - abs(closing_balance))

            # Pending amount (small random amount sometimes)
            pending_amount = None
            if rng.random() < 0.05:
                pending_amount = round(rng.uniform(10, 500), 2)

            row = {
                "balance_id": balance_id,
                "account_id": account_id,
                "balance_date": d,
                "opening_balance": round(opening_balance_today, 2),
                "closing_balance": closing_balance,
                "daily_credits": round(daily_credits, 2),
                "daily_debits": round(daily_debits, 2),
                "daily_net_change": round(daily_net_change, 2),
                "transaction_count": txn_data["count"],
                "available_credit": available_credit,
                "pending_amount": pending_amount,
            }
            rows.append(row)

            current_balance = closing_balance

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

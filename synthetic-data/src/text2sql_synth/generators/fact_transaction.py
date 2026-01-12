"""Generator for fact_transaction fact table.

Core transaction fact table with multi-table joins, seasonality, and risk correlation.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "fact_transaction"

# Transaction channels
CHANNELS = ["online", "pos", "mobile", "atm", "phone"]
CHANNEL_WEIGHTS = [0.40, 0.30, 0.20, 0.07, 0.03]

# Transaction statuses
STATUSES = ["approved", "declined", "pending", "failed"]

# Currency codes
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "MXN"]
CURRENCY_WEIGHTS = [0.92, 0.03, 0.02, 0.02, 0.01]

# Risk tier multipliers for decline rate
RISK_DECLINE_MULTIPLIERS = {
    "low": 0.5,
    "medium": 1.0,
    "high": 2.0,
    "critical": 4.0,
}


def _compute_activity_propensity(
    rng: np.random.Generator,
    customer_activity: float,
    merchant_popularity: int,
    alpha: float,
) -> float:
    """Compute combined activity propensity for customer-merchant pair.

    Uses a combination of customer activity score and merchant popularity
    to determine likelihood of transactions.
    """
    # Normalize merchant popularity to 0-1 range (assuming max ~1000)
    merchant_score = min(merchant_popularity / 100.0, 10.0)

    # Combined propensity (multiplicative with some noise)
    base_propensity = customer_activity * merchant_score
    noise = 0.8 + rng.random() * 0.4  # 0.8 to 1.2 noise factor

    return base_propensity * noise


def _get_weekday_factor(day_of_week: int) -> float:
    """Get transaction volume factor based on day of week.

    Weekend days typically have different spending patterns.
    """
    # Monday=0, Sunday=6
    factors = [0.95, 1.0, 1.0, 1.05, 1.15, 1.20, 1.10]
    return factors[day_of_week]


def _generate_transaction_time(
    rng: np.random.Generator,
    base_date: datetime,
) -> datetime:
    """Generate a realistic transaction timestamp within the day.

    Peak hours are during business hours and evening.
    """
    # Hour distribution weights (24 hours)
    hour_weights = [
        0.02, 0.01, 0.01, 0.01, 0.01, 0.02,  # 0-5 AM (very low)
        0.03, 0.05, 0.06, 0.07, 0.08, 0.09,  # 6-11 AM (building up)
        0.10, 0.09, 0.08, 0.07, 0.06, 0.07,  # 12-5 PM (peak to stable)
        0.08, 0.09, 0.08, 0.06, 0.04, 0.03,  # 6-11 PM (evening peak then decline)
    ]
    hour_weights = np.array(hour_weights)
    hour_weights = hour_weights / hour_weights.sum()

    hour = rng.choice(24, p=hour_weights)
    minute = rng.integers(0, 60)
    second = rng.integers(0, 60)
    microsecond = rng.integers(0, 1000000)

    return base_date.replace(
        hour=int(hour),
        minute=int(minute),
        second=int(second),
        microsecond=int(microsecond),
    )


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the fact_transaction fact table.

    Creates transaction records with:
    - transaction_id: Unique identifier
    - account_id: FK to dim_account
    - customer_id: FK to dim_customer
    - merchant_id: FK to dim_merchant
    - counterparty_id: FK to dim_counterparty (nullable for non-merchant txns)
    - institution_id: FK to dim_institution (issuer)
    - time_id: FK to dim_time (date_key)
    - transaction_ts: Full timestamp
    - gross_amount: Transaction amount before fees
    - fee_amount: Processing fees
    - net_amount: gross_amount - fee_amount
    - currency: Transaction currency
    - channel: online, pos, mobile, atm, phone
    - status: approved, declined, pending, failed
    - risk_tier: Transaction risk assessment
    - device_id: Device identifier (nullable)
    - is_emulator: Whether device is flagged as emulator
    - is_fraud_flagged: Whether transaction is fraud-flagged

    Transaction volume is driven by:
    - Seasonality from dim_time
    - Weekday patterns
    - Customer activity propensity (long-tail)
    - Merchant popularity (Zipf)

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with rates and distributions.

    Returns:
        DataFrame with transaction fact data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get required dimension tables
    time_df = ctx.get_table("dim_time")
    customer_df = ctx.get_table("dim_customer")
    account_df = ctx.get_table("dim_account")
    merchant_df = ctx.get_table("dim_merchant")
    counterparty_df = ctx.get_table("dim_counterparty")
    institution_df = ctx.get_table("dim_institution")

    if any(df is None for df in [time_df, customer_df, account_df, merchant_df]):
        raise ValueError("Dimension tables must be generated before fact_transaction")

    # Build lookup structures
    account_to_customer = dict(zip(account_df["account_id"], account_df["customer_id"]))
    account_to_institution = dict(zip(account_df["account_id"], account_df["institution_id"]))
    account_risk = dict(zip(account_df["account_id"], account_df["risk_tier"]))

    customer_activity = dict(zip(customer_df["customer_id"], customer_df["activity_score"]))
    customer_accounts = account_df.groupby("customer_id")["account_id"].apply(list).to_dict()

    merchant_popularity = dict(zip(merchant_df["merchant_id"], merchant_df["popularity_score"]))
    active_merchants = merchant_df[merchant_df["is_active"] == True]["merchant_id"].tolist()

    # Build counterparty lookup for merchants
    if counterparty_df is not None:
        merchant_counterparty = dict(zip(
            counterparty_df[counterparty_df["merchant_id"].notna()]["merchant_id"],
            counterparty_df[counterparty_df["merchant_id"].notna()]["counterparty_id"],
        ))
    else:
        merchant_counterparty = {}

    # Get time dimension data
    time_seasonality = dict(zip(time_df["date_key"], time_df["seasonality_factor"]))
    time_dates = dict(zip(time_df["date_key"], time_df["full_date"]))
    time_weekday = dict(zip(time_df["date_key"], time_df["day_of_week"]))

    # Calculate total transactions to generate
    num_days = len(time_df)
    base_txns_per_day = cfg.scale.txns_per_day

    # Pre-compute merchant selection probabilities based on popularity
    merchant_ids = active_merchants
    merchant_pops = np.array([merchant_popularity.get(m, 1) for m in merchant_ids], dtype=float)
    merchant_probs = merchant_pops / merchant_pops.sum()

    # Pre-compute active customers and their accounts
    active_customers = customer_df[customer_df["is_active"] == True]["customer_id"].tolist()

    rows = []
    for _, time_row in time_df.iterrows():
        date_key = time_row["date_key"]
        full_date = time_row["full_date"]
        seasonality = time_row["seasonality_factor"]
        day_of_week = time_row["day_of_week"]

        # Adjust transaction count for seasonality and weekday
        weekday_factor = _get_weekday_factor(day_of_week)
        adjusted_txns = int(base_txns_per_day * seasonality * weekday_factor)

        # Add some random variance (+/- 10%)
        adjusted_txns = int(adjusted_txns * (0.9 + rng.random() * 0.2))

        for _ in range(adjusted_txns):
            transaction_id = ctx.stable_id("txn")

            # Select customer weighted by activity (long-tail)
            # Use activity scores as weights
            customer_activities = np.array([
                customer_activity.get(c, 1.0) for c in active_customers
            ], dtype=float)
            customer_probs = customer_activities / customer_activities.sum()
            customer_id = active_customers[rng.choice(len(active_customers), p=customer_probs)]

            # Get customer's accounts
            accounts = customer_accounts.get(customer_id, [])
            if not accounts:
                continue

            # Select account (random among customer's accounts)
            account_id = accounts[rng.integers(0, len(accounts))]
            institution_id = account_to_institution.get(account_id)
            account_risk_tier = account_risk.get(account_id, "low")

            # Select merchant weighted by popularity (Zipf)
            merchant_id = merchant_ids[rng.choice(len(merchant_ids), p=merchant_probs)]
            counterparty_id = merchant_counterparty.get(merchant_id)

            # Generate transaction timestamp
            base_datetime = datetime.combine(full_date, datetime.min.time())
            transaction_ts = _generate_transaction_time(rng, base_datetime)

            # Generate amount (Pareto distribution)
            merchant_avg = merchant_df[
                merchant_df["merchant_id"] == merchant_id
            ]["avg_transaction_amount"].iloc[0]
            gross_amount = round(
                float(ctx.sample_pareto(rng, cfg.distribution.transaction_amount_pareto_alpha, scale=merchant_avg * 0.5)),
                2
            )
            # Cap at reasonable maximum
            gross_amount = min(gross_amount, 10000.0)

            # Fee (typically 2-3% for card transactions)
            fee_rate = 0.02 + rng.random() * 0.01
            fee_amount = round(gross_amount * fee_rate, 2)
            net_amount = round(gross_amount - fee_amount, 2)

            # Currency
            currency = ctx.sample_categorical(rng, CURRENCIES, weights=CURRENCY_WEIGHTS)

            # Channel
            channel = ctx.sample_categorical(rng, CHANNELS, weights=CHANNEL_WEIGHTS)

            # Status - apply risk-correlated decline rate
            base_decline_rate = cfg.rates.decline_rate
            risk_multiplier = RISK_DECLINE_MULTIPLIERS.get(account_risk_tier, 1.0)
            effective_decline_rate = min(base_decline_rate * risk_multiplier, 0.5)

            if rng.random() < effective_decline_rate:
                status = "declined"
            elif rng.random() < 0.005:  # Small chance of pending/failed
                status = ctx.sample_categorical(rng, ["pending", "failed"], weights=[0.7, 0.3])
            else:
                status = "approved"

            # Device ID (80% have device info)
            device_id = None
            if rng.random() < 0.8:
                device_id = f"dev_{rng.integers(100000, 999999)}"

            # Emulator flag - higher for high-risk, only if device present
            is_emulator = False
            if device_id:
                emulator_rate = cfg.rates.emulator_rate * risk_multiplier
                is_emulator = rng.random() < emulator_rate

            # Fraud flag - correlated with risk tier
            fraud_rate = cfg.rates.fraud_rate * risk_multiplier
            is_fraud_flagged = rng.random() < fraud_rate

            row = {
                "transaction_id": transaction_id,
                "account_id": account_id,
                "customer_id": customer_id,
                "merchant_id": merchant_id,
                "counterparty_id": counterparty_id,
                "institution_id": institution_id,
                "time_id": date_key,
                "transaction_ts": transaction_ts,
                "gross_amount": gross_amount,
                "fee_amount": fee_amount,
                "net_amount": net_amount,
                "currency": currency,
                "channel": channel,
                "status": status,
                "risk_tier": account_risk_tier,
                "device_id": device_id,
                "is_emulator": is_emulator,
                "is_fraud_flagged": is_fraud_flagged,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

"""Generator for dim_account dimension table.

Generates account records linked to customers and institutions.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_account"

ACCOUNT_TYPES = ["checking", "savings", "credit", "investment", "money_market"]
ACCOUNT_TYPE_WEIGHTS = [0.40, 0.25, 0.25, 0.07, 0.03]

ACCOUNT_STATUSES = ["active", "inactive", "suspended", "closed"]
ACCOUNT_STATUS_WEIGHTS = [0.92, 0.04, 0.02, 0.02]

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "MXN"]
CURRENCY_WEIGHTS = [0.90, 0.04, 0.03, 0.02, 0.01]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_account dimension table.

    Creates account records with:
    - account_id: Unique identifier
    - customer_id: FK to dim_customer
    - institution_id: FK to dim_institution (issuer)
    - account_type: checking, savings, credit, etc.
    - account_number: Masked account number
    - account_status: active, inactive, suspended, closed
    - currency: Account currency code
    - opened_date: When account was opened
    - closed_date: When account was closed (nullable)
    - credit_limit: For credit accounts (nullable)
    - risk_tier: Inherited or independent risk assessment

    Number of accounts per customer varies based on config.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale parameters.

    Returns:
        DataFrame with account dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get customer data
    customer_df = ctx.get_table("dim_customer")
    if customer_df is None or len(customer_df) == 0:
        raise ValueError("dim_customer must be generated before dim_account")

    customer_ids = customer_df["customer_id"].tolist()
    customer_risk = dict(zip(customer_df["customer_id"], customer_df["risk_tier"]))
    customer_since = dict(zip(customer_df["customer_id"], customer_df["customer_since"]))

    # Get institution data (filter to issuers)
    institution_df = ctx.get_table("dim_institution")
    if institution_df is None or len(institution_df) == 0:
        raise ValueError("dim_institution must be generated before dim_account")

    issuer_ids = institution_df[institution_df["institution_type"] == "issuer"][
        "institution_id"
    ].tolist()

    if len(issuer_ids) == 0:
        raise ValueError("No issuer institutions found")

    rows = []
    for customer_id in customer_ids:
        # Determine number of accounts for this customer
        num_accounts = rng.integers(
            cfg.scale.accounts_per_customer_min,
            cfg.scale.accounts_per_customer_max + 1,
        )

        cust_since = customer_since[customer_id]
        cust_risk = customer_risk[customer_id]

        for _ in range(num_accounts):
            account_id = ctx.stable_id("acct")

            # Assign to a random issuer
            institution_id = issuer_ids[rng.integers(0, len(issuer_ids))]

            # Account type
            account_type = ctx.sample_categorical(rng, ACCOUNT_TYPES, weights=ACCOUNT_TYPE_WEIGHTS)

            # Generate masked account number (last 4 visible)
            last_four = f"{rng.integers(0, 9999):04d}"
            account_number = f"****{last_four}"

            # Account status
            account_status = ctx.sample_categorical(
                rng, ACCOUNT_STATUSES, weights=ACCOUNT_STATUS_WEIGHTS
            )

            # Currency
            currency = ctx.sample_categorical(rng, CURRENCIES, weights=CURRENCY_WEIGHTS)

            # Opened date (after customer_since, before time window start)
            days_range = (cfg.time_window.start_date - cust_since).days
            if days_range > 0:
                opened_offset = rng.integers(0, days_range)
            else:
                opened_offset = 0
            opened_date = cust_since + timedelta(days=int(opened_offset))

            # Closed date (only if closed)
            closed_date = None
            if account_status == "closed":
                days_open = (cfg.time_window.start_date - opened_date).days
                if days_open > 30:
                    close_offset = rng.integers(30, days_open)
                    closed_date = opened_date + timedelta(days=int(close_offset))

            # Credit limit (only for credit accounts)
            credit_limit = None
            if account_type == "credit":
                # Credit limits in thousands, Pareto distributed
                base_limit = ctx.sample_pareto(rng, 2.5, scale=1000.0)
                credit_limit = round(min(float(base_limit), 100000.0), -2)  # Round to nearest 100

            # Risk tier (80% inherits from customer, 20% independent)
            if rng.random() < 0.8:
                risk_tier = cust_risk
            else:
                risk_tier = ctx.sample_categorical(
                    rng,
                    ["low", "medium", "high", "critical"],
                    weights=cfg.distribution.risk_tier_weights,
                )

            row = {
                "account_id": account_id,
                "customer_id": customer_id,
                "institution_id": institution_id,
                "account_type": account_type,
                "account_number": account_number,
                "account_status": account_status,
                "currency": currency,
                "opened_date": opened_date,
                "closed_date": closed_date,
                "credit_limit": credit_limit,
                "risk_tier": risk_tier,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

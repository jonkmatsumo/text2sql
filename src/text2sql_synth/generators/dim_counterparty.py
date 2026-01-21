"""Generator for dim_counterparty dimension table.

Generates counterparty records that represent the other side of a transaction.
Links to merchants but provides a unified view for P2P, B2B, etc.
"""

from __future__ import annotations

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_counterparty"

COUNTERPARTY_TYPES = ["merchant", "individual", "business", "government", "internal"]
COUNTERPARTY_TYPE_WEIGHTS = [0.85, 0.08, 0.04, 0.02, 0.01]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_counterparty dimension table.

    Creates counterparty records that unify different transaction recipients:
    - counterparty_id: Unique identifier
    - counterparty_type: merchant, individual, business, government, internal
    - counterparty_name: Display name
    - merchant_id: FK to dim_merchant (nullable, only for merchant type)
    - external_id: External reference ID (nullable)
    - risk_tier: Risk assessment
    - is_verified: Whether identity is verified
    - country_code: Country of counterparty

    Most counterparties are merchants (linking to dim_merchant), but some
    represent P2P transfers, bill payments, etc.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale parameters.

    Returns:
        DataFrame with counterparty dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get merchant data
    merchant_df = ctx.get_table("dim_merchant")
    if merchant_df is None or len(merchant_df) == 0:
        raise ValueError("dim_merchant must be generated before dim_counterparty")

    # Sample names for non-merchant counterparties
    individual_names = [
        "John Smith",
        "Jane Doe",
        "Robert Johnson",
        "Emily Davis",
        "Michael Brown",
        "Sarah Wilson",
        "David Miller",
        "Jennifer Taylor",
    ]

    business_names = [
        "ABC Corporation",
        "XYZ Holdings",
        "Smith & Associates",
        "Johnson Enterprises",
        "Metro Services LLC",
        "Tech Solutions Inc",
    ]

    government_names = [
        "US Treasury",
        "State Tax Authority",
        "City Utilities",
        "County Services",
        "Federal Agency",
        "Municipal Court",
    ]

    internal_names = [
        "Internal Transfer",
        "System Adjustment",
        "Fee Assessment",
        "Interest Payment",
        "Rewards Credit",
        "Cashback",
    ]

    rows = []

    # First, create counterparties for all merchants
    for _, merchant_row in merchant_df.iterrows():
        counterparty_id = ctx.stable_id("cpty")

        row = {
            "counterparty_id": counterparty_id,
            "counterparty_type": "merchant",
            "counterparty_name": merchant_row["merchant_name"],
            "merchant_id": merchant_row["merchant_id"],
            "external_id": merchant_row["mcc_code"],
            "risk_tier": merchant_row["risk_tier"],
            "is_verified": True,
            "country_code": "US",
        }
        rows.append(row)

    # Add non-merchant counterparties
    # Number based on scale (roughly 10% of merchants for each type)
    non_merchant_count = max(10, cfg.scale.merchants // 10)

    # Individuals
    for i in range(non_merchant_count):
        counterparty_id = ctx.stable_id("cpty")
        name = individual_names[i % len(individual_names)]
        # Add suffix to make unique
        name = f"{name} ({counterparty_id.split('_')[1]})"

        row = {
            "counterparty_id": counterparty_id,
            "counterparty_type": "individual",
            "counterparty_name": name,
            "merchant_id": None,
            "external_id": f"P2P-{rng.integers(10000, 99999)}",
            "risk_tier": ctx.sample_categorical(rng, ["low", "medium"], weights=[0.9, 0.1]),
            "is_verified": rng.random() < 0.7,
            "country_code": ctx.sample_categorical(
                rng, ["US", "CA", "MX", "GB"], weights=[0.9, 0.05, 0.03, 0.02]
            ),
        }
        rows.append(row)

    # Businesses
    for i in range(non_merchant_count // 2):
        counterparty_id = ctx.stable_id("cpty")
        name = business_names[i % len(business_names)]
        name = f"{name} ({counterparty_id.split('_')[1]})"

        row = {
            "counterparty_id": counterparty_id,
            "counterparty_type": "business",
            "counterparty_name": name,
            "merchant_id": None,
            "external_id": f"B2B-{rng.integers(10000, 99999)}",
            "risk_tier": ctx.sample_categorical(
                rng, ["low", "medium", "high"], weights=[0.7, 0.25, 0.05]
            ),
            "is_verified": True,
            "country_code": "US",
        }
        rows.append(row)

    # Government
    for i in range(min(len(government_names), non_merchant_count // 4)):
        counterparty_id = ctx.stable_id("cpty")
        name = government_names[i]

        row = {
            "counterparty_id": counterparty_id,
            "counterparty_type": "government",
            "counterparty_name": name,
            "merchant_id": None,
            "external_id": f"GOV-{rng.integers(1000, 9999)}",
            "risk_tier": "low",
            "is_verified": True,
            "country_code": "US",
        }
        rows.append(row)

    # Internal
    for i in range(len(internal_names)):
        counterparty_id = ctx.stable_id("cpty")
        name = internal_names[i]

        row = {
            "counterparty_id": counterparty_id,
            "counterparty_type": "internal",
            "counterparty_name": name,
            "merchant_id": None,
            "external_id": f"INT-{i + 1:03d}",
            "risk_tier": "low",
            "is_verified": True,
            "country_code": "US",
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

"""Generator for dim_merchant dimension table.

Generates merchant records with realistic MCC codes, risk profiles, and
long-tail popularity distributions.
"""

from __future__ import annotations

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_merchant"

# Merchant Category Codes (MCC) with descriptions and base risk
MCC_CATEGORIES = [
    ("5411", "Grocery Stores", "low", 0.15),
    ("5541", "Gas Stations", "low", 0.10),
    ("5812", "Restaurants", "low", 0.12),
    ("5814", "Fast Food", "low", 0.08),
    ("5311", "Department Stores", "low", 0.06),
    ("5912", "Drug Stores", "low", 0.05),
    ("7011", "Hotels/Motels", "medium", 0.04),
    ("4111", "Transportation", "low", 0.03),
    ("5651", "Clothing Stores", "low", 0.05),
    ("5732", "Electronics Stores", "medium", 0.04),
    ("5999", "Misc Retail", "low", 0.06),
    ("7832", "Movie Theaters", "low", 0.02),
    ("7941", "Sports Events", "medium", 0.02),
    ("5942", "Book Stores", "low", 0.02),
    ("5691", "Mens/Womens Clothing", "low", 0.03),
    ("5310", "Discount Stores", "low", 0.04),
    ("5331", "Variety Stores", "low", 0.02),
    ("5921", "Liquor Stores", "medium", 0.02),
    ("7922", "Ticket Agencies", "high", 0.01),
    ("5962", "Telemarketing", "high", 0.01),
    ("5967", "Direct Marketing", "high", 0.01),
    ("7995", "Gambling", "critical", 0.01),
    ("5993", "Cigar/Tobacco", "medium", 0.01),
]

MERCHANT_NAME_PREFIXES = [
    "The",
    "Quick",
    "Super",
    "Best",
    "Prime",
    "Value",
    "Express",
    "Local",
    "Metro",
    "City",
    "Town",
    "Village",
    "Corner",
    "Central",
    "Main",
]

MERCHANT_NAME_SUFFIXES = [
    "Mart",
    "Store",
    "Shop",
    "Center",
    "Place",
    "Depot",
    "World",
    "Plus",
    "Pro",
    "Zone",
    "Hub",
    "Point",
    "Stop",
    "Corner",
    "Market",
]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_merchant dimension table.

    Creates merchant records with:
    - merchant_id: Unique identifier
    - merchant_name: Business name
    - mcc_code: Merchant Category Code
    - mcc_description: Human-readable MCC description
    - address_id: FK to dim_address
    - acquirer_id: FK to dim_institution (acquirer)
    - risk_tier: low, medium, high, critical
    - is_active: Whether merchant is active
    - popularity_score: Zipf-distributed popularity
    - avg_transaction_amount: Typical transaction size
    - established_date: When merchant started accepting cards

    Long-tail popularity_score ensures realistic transaction concentration.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale and distribution parameters.

    Returns:
        DataFrame with merchant dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)
    num_merchants = cfg.scale.merchants

    # Get address data
    address_df = ctx.get_table("dim_address")
    if address_df is not None and len(address_df) > 0:
        # Prefer commercial addresses for merchants
        commercial_addresses = address_df[address_df["address_type"].isin(["commercial", "mixed"])][
            "address_id"
        ].tolist()
        if len(commercial_addresses) < num_merchants:
            # Fall back to all addresses
            commercial_addresses = address_df["address_id"].tolist()
        address_ids = commercial_addresses
    else:
        address_ids = None

    # Get acquirer institutions
    institution_df = ctx.get_table("dim_institution")
    if institution_df is not None and len(institution_df) > 0:
        acquirer_ids = institution_df[institution_df["institution_type"] == "acquirer"][
            "institution_id"
        ].tolist()
    else:
        acquirer_ids = None

    # Prepare MCC sampling
    mcc_codes = [m[0] for m in MCC_CATEGORIES]
    mcc_descs = {m[0]: m[1] for m in MCC_CATEGORIES}
    mcc_risks = {m[0]: m[2] for m in MCC_CATEGORIES}
    mcc_weights = [m[3] for m in MCC_CATEGORIES]

    rows = []
    for _ in range(num_merchants):
        merchant_id = ctx.stable_id("merch")

        # Generate merchant name
        prefix = MERCHANT_NAME_PREFIXES[rng.integers(0, len(MERCHANT_NAME_PREFIXES))]
        suffix = MERCHANT_NAME_SUFFIXES[rng.integers(0, len(MERCHANT_NAME_SUFFIXES))]
        # Sometimes add a location element
        if rng.random() < 0.3:
            location = ctx.sample_categorical(
                rng,
                ["Downtown", "Westside", "Eastside", "North", "South", "Central", "Village"],
            )
            merchant_name = f"{location} {prefix} {suffix}"
        else:
            merchant_name = f"{prefix} {suffix}"

        # Ensure unique names by adding ID suffix
        id_suffix = merchant_id.split("_")[1]
        merchant_name = f"{merchant_name} #{id_suffix}"

        # Sample MCC category
        mcc_idx = rng.choice(len(mcc_codes), p=[w / sum(mcc_weights) for w in mcc_weights])
        mcc_code = mcc_codes[mcc_idx]
        mcc_description = mcc_descs[mcc_code]
        base_risk = mcc_risks[mcc_code]

        # Address
        address_id = None
        if address_ids:
            address_id = address_ids[rng.integers(0, len(address_ids))]

        # Acquirer
        acquirer_id = None
        if acquirer_ids:
            acquirer_id = acquirer_ids[rng.integers(0, len(acquirer_ids))]

        # Risk tier (mostly follows MCC risk, some variance)
        if rng.random() < 0.85:
            risk_tier = base_risk
        else:
            risk_tier = ctx.sample_categorical(
                rng,
                ["low", "medium", "high", "critical"],
                weights=cfg.distribution.risk_tier_weights,
            )

        # Active status
        is_active = rng.random() < 0.92

        # Popularity score: Zipf distribution for long-tail
        # Higher score = more transactions will go to this merchant
        popularity_score = int(
            ctx.sample_zipf(
                rng,
                cfg.distribution.merchant_popularity_zipf_alpha,
                min_val=1,
                max_val=1000,
            )
        )

        # Average transaction amount (varies by MCC)
        base_amounts = {
            "5411": 75.0,
            "5541": 45.0,
            "5812": 35.0,
            "5814": 15.0,
            "5311": 85.0,
            "5912": 25.0,
            "7011": 150.0,
            "4111": 25.0,
            "5651": 65.0,
            "5732": 250.0,
            "5999": 50.0,
            "7832": 30.0,
            "7941": 75.0,
            "5942": 25.0,
            "5691": 80.0,
            "5310": 45.0,
            "5331": 20.0,
            "5921": 35.0,
            "7922": 100.0,
            "5962": 50.0,
            "5967": 75.0,
            "7995": 100.0,
            "5993": 15.0,
        }
        base_avg = base_amounts.get(mcc_code, 50.0)
        # Add some variance
        avg_transaction_amount = round(base_avg * (0.5 + rng.random()), 2)

        # Established date (up to 20 years ago)
        from datetime import timedelta

        days_ago = rng.integers(0, 20 * 365)
        established_date = cfg.time_window.start_date - timedelta(days=int(days_ago))

        row = {
            "merchant_id": merchant_id,
            "merchant_name": merchant_name,
            "mcc_code": mcc_code,
            "mcc_description": mcc_description,
            "address_id": address_id,
            "acquirer_id": acquirer_id,
            "risk_tier": risk_tier,
            "is_active": is_active,
            "popularity_score": popularity_score,
            "avg_transaction_amount": avg_transaction_amount,
            "established_date": established_date,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

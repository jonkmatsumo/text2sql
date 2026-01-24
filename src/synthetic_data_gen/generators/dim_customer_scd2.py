"""Generator for dim_customer_scd2 dimension table.

Generates slowly changing dimension (Type 2) records for customer history.
Tracks changes in customer attributes over time.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from synthetic_data_gen.config import SynthConfig
from synthetic_data_gen.context import GenerationContext

TABLE_NAME = "dim_customer_scd2"

# Attributes that can change over time
CHANGEABLE_ATTRIBUTES = ["risk_tier", "customer_segment", "is_active"]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_customer_scd2 slowly changing dimension table.

    Creates historical customer records with:
    - scd_id: Unique identifier for this version
    - customer_id: FK to dim_customer (business key)
    - first_name: Customer first name
    - last_name: Customer last name
    - email: Customer email
    - risk_tier: Risk tier at this point in time
    - customer_segment: Segment at this point in time
    - is_active: Active status at this point in time
    - effective_from: When this version became effective
    - effective_to: When this version was superseded (null for current)
    - is_current: True for the current version
    - version_number: Sequential version number

    Some customers have multiple historical records showing changes.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale parameters.

    Returns:
        DataFrame with SCD2 data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get customer data
    customer_df = ctx.get_table("dim_customer")
    if customer_df is None or len(customer_df) == 0:
        raise ValueError("dim_customer must be generated before dim_customer_scd2")

    rows = []

    for _, cust_row in customer_df.iterrows():
        customer_id = cust_row["customer_id"]
        customer_since = cust_row["customer_since"]

        # Start with initial version
        current_risk = cust_row["risk_tier"]
        current_segment = cust_row["customer_segment"]
        current_active = cust_row["is_active"]

        # Decide how many historical versions (most have 1, some have 2-4)
        num_changes = 0
        if rng.random() < 0.20:  # 20% have at least one change
            num_changes = int(ctx.sample_zipf(rng, 2.5, min_val=1, max_val=4))

        versions = []
        version_start = customer_since

        # Generate historical versions (working backwards from current)
        if num_changes > 0:
            # Calculate time points for changes
            total_days = (cfg.time_window.start_date - customer_since).days

            # Need at least some buffer to generate versions
            min_days_total = (num_changes + 1) * 10
            if total_days >= min_days_total:
                # Use a smaller buffer (10 days) to allow more flexibility with short tenures
                change_points = sorted(
                    rng.choice(range(10, total_days - 10), size=num_changes, replace=False)
                )

                # Build versions from oldest to newest
                prev_risk = current_risk
                prev_segment = current_segment
                prev_active = current_active

                # Randomly vary historical values
                risk_options = ["low", "medium", "high", "critical"]
                segment_options = ["retail", "premium", "business", "vip"]

                for i, days_offset in enumerate(change_points):
                    # This version ends at the change point
                    version_end = customer_since + timedelta(days=int(days_offset))

                    # Randomly pick which attribute changed
                    changed_attr = rng.choice(CHANGEABLE_ATTRIBUTES)

                    if changed_attr == "risk_tier":
                        # Pick a different risk tier
                        old_risk = rng.choice([r for r in risk_options if r != prev_risk])
                        versions.append(
                            {
                                "risk_tier": old_risk,
                                "customer_segment": prev_segment,
                                "is_active": prev_active,
                                "effective_from": version_start,
                                "effective_to": version_end,
                            }
                        )
                        prev_risk = old_risk
                    elif changed_attr == "customer_segment":
                        old_segment = rng.choice([s for s in segment_options if s != prev_segment])
                        versions.append(
                            {
                                "risk_tier": prev_risk,
                                "customer_segment": old_segment,
                                "is_active": prev_active,
                                "effective_from": version_start,
                                "effective_to": version_end,
                            }
                        )
                        prev_segment = old_segment
                    else:  # is_active
                        versions.append(
                            {
                                "risk_tier": prev_risk,
                                "customer_segment": prev_segment,
                                "is_active": not prev_active,
                                "effective_from": version_start,
                                "effective_to": version_end,
                            }
                        )
                        prev_active = not prev_active

                    version_start = version_end

        # Add current version
        versions.append(
            {
                "risk_tier": current_risk,
                "customer_segment": current_segment,
                "is_active": current_active,
                "effective_from": version_start,
                "effective_to": None,
            }
        )

        # Create rows for all versions
        for version_num, version in enumerate(versions, start=1):
            scd_id = ctx.stable_id("scd")

            row = {
                "scd_id": scd_id,
                "customer_id": customer_id,
                "first_name": cust_row["first_name"],
                "last_name": cust_row["last_name"],
                "email": cust_row["email"],
                "risk_tier": version["risk_tier"],
                "customer_segment": version["customer_segment"],
                "is_active": version["is_active"],
                "effective_from": version["effective_from"],
                "effective_to": version["effective_to"],
                "is_current": version["effective_to"] is None,
                "version_number": version_num,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

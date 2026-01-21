"""Generator for bridge_customer_address bridge table.

Links customers to their addresses (primary, billing, shipping, etc.).
Creates the geographic ambiguity needed for realistic test scenarios.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "bridge_customer_address"

ADDRESS_TYPES = ["primary", "billing", "shipping", "mailing", "work"]
ADDRESS_TYPE_WEIGHTS = [1.0, 0.7, 0.5, 0.3, 0.2]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the bridge_customer_address bridge table.

    Creates address assignments with:
    - bridge_id: Unique identifier for the relationship
    - customer_id: FK to dim_customer
    - address_id: FK to dim_address
    - address_type: primary, billing, shipping, mailing, work
    - is_current: Whether this is the current address for this type
    - effective_from: When this address became effective
    - effective_to: When this address was superseded (nullable)

    Each customer has at least a primary address. Many have multiple
    address types, and some share addresses (same household, etc.).

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale parameters.

    Returns:
        DataFrame with bridge data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get customer data
    customer_df = ctx.get_table("dim_customer")
    if customer_df is None or len(customer_df) == 0:
        raise ValueError("dim_customer must be generated before bridge_customer_address")

    customer_ids = customer_df["customer_id"].tolist()
    customer_since = dict(zip(customer_df["customer_id"], customer_df["customer_since"]))
    primary_address = dict(zip(customer_df["customer_id"], customer_df["primary_address_id"]))

    # Get address data
    address_df = ctx.get_table("dim_address")
    if address_df is None or len(address_df) == 0:
        raise ValueError("dim_address must be generated before bridge_customer_address")

    address_ids = address_df["address_id"].tolist()

    rows = []
    for customer_id in customer_ids:
        cust_since = customer_since[customer_id]
        prim_addr = primary_address[customer_id]

        # Always add primary address
        if prim_addr:
            bridge_id = ctx.stable_id("custaddr")
            row = {
                "bridge_id": bridge_id,
                "customer_id": customer_id,
                "address_id": prim_addr,
                "address_type": "primary",
                "is_current": True,
                "effective_from": cust_since,
                "effective_to": None,
            }
            rows.append(row)

        # Randomly add other address types
        for addr_type, weight in zip(ADDRESS_TYPES[1:], ADDRESS_TYPE_WEIGHTS[1:]):
            if rng.random() < weight:
                bridge_id = ctx.stable_id("custaddr")

                # Sometimes same as primary, sometimes different
                if rng.random() < 0.6 and prim_addr:
                    addr_id = prim_addr
                else:
                    addr_id = address_ids[rng.integers(0, len(address_ids))]

                # Effective date
                days_range = (cfg.time_window.start_date - cust_since).days
                if days_range > 0:
                    offset = rng.integers(0, days_range)
                else:
                    offset = 0
                effective_from = cust_since + timedelta(days=int(offset))

                row = {
                    "bridge_id": bridge_id,
                    "customer_id": customer_id,
                    "address_id": addr_id,
                    "address_type": addr_type,
                    "is_current": True,
                    "effective_from": effective_from,
                    "effective_to": None,
                }
                rows.append(row)

        # Some customers have historical addresses (moved)
        if rng.random() < 0.15 and prim_addr:
            # Old primary address
            bridge_id = ctx.stable_id("custaddr")
            old_addr_id = address_ids[rng.integers(0, len(address_ids))]

            # Old address was effective from customer_since until sometime before now
            days_range = (cfg.time_window.start_date - cust_since).days
            if days_range > 30:
                end_offset = rng.integers(30, days_range)
                effective_to = cust_since + timedelta(days=int(end_offset))

                row = {
                    "bridge_id": bridge_id,
                    "customer_id": customer_id,
                    "address_id": old_addr_id,
                    "address_type": "primary",
                    "is_current": False,
                    "effective_from": cust_since,
                    "effective_to": effective_to,
                }
                rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

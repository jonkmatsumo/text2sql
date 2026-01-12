"""Generator for dim_institution dimension table.

Generates financial institution records (banks, processors, issuers).
"""

from __future__ import annotations

import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_institution"

# Sample institution data for realistic generation
INSTITUTION_TYPES = ["issuer", "acquirer", "processor", "network"]

INSTITUTION_NAMES = {
    "issuer": [
        "Chase Bank", "Bank of America", "Wells Fargo", "Citibank", "Capital One",
        "US Bank", "PNC Bank", "TD Bank", "Truist", "Fifth Third Bank",
        "Ally Bank", "Discover Bank", "Marcus by Goldman Sachs", "Synchrony Bank",
    ],
    "acquirer": [
        "Worldpay", "Fiserv", "Global Payments", "TSYS", "Elavon",
        "Heartland Payment", "Square", "Stripe", "PayPal", "Adyen",
    ],
    "processor": [
        "FIS", "Fiserv", "TSYS", "First Data", "Worldline",
        "ACI Worldwide", "Jack Henry", "Finastra", "NCR",
    ],
    "network": [
        "Visa", "Mastercard", "American Express", "Discover Network",
    ],
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_institution dimension table.

    Creates institution records with:
    - institution_id: Unique identifier
    - institution_name: Name of the institution
    - institution_type: issuer, acquirer, processor, network
    - country_code: ISO country code (US-focused)
    - is_active: Whether institution is currently active
    - routing_number: For banks (nullable)
    - swift_code: For international (nullable)

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration (used for consistency).

    Returns:
        DataFrame with institution dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    rows = []

    # Generate institutions of each type
    for inst_type in INSTITUTION_TYPES:
        names = INSTITUTION_NAMES[inst_type]
        for name in names:
            institution_id = ctx.stable_id("inst")

            # Generate routing number for issuers (9 digits)
            routing_number = None
            if inst_type == "issuer":
                routing_number = f"{rng.integers(100000000, 999999999)}"

            # Generate SWIFT code for some institutions
            swift_code = None
            if rng.random() > 0.5:
                # SWIFT: 8 or 11 characters
                swift_code = "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), size=4))
                swift_code += "US"  # Country
                swift_code += "".join(rng.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"), size=2))

            row = {
                "institution_id": institution_id,
                "institution_name": name,
                "institution_type": inst_type,
                "country_code": "US",
                "is_active": True,
                "routing_number": routing_number,
                "swift_code": swift_code,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

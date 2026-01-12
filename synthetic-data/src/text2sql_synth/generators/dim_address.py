"""Generator for dim_address dimension table.

Generates address records that can be used by customers and merchants,
creating geographic ambiguity for realistic test scenarios.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_address"

# US State data with population weights for realistic distribution
US_STATES = [
    ("CA", "California", 0.118),
    ("TX", "Texas", 0.088),
    ("FL", "Florida", 0.066),
    ("NY", "New York", 0.059),
    ("PA", "Pennsylvania", 0.039),
    ("IL", "Illinois", 0.038),
    ("OH", "Ohio", 0.035),
    ("GA", "Georgia", 0.032),
    ("NC", "North Carolina", 0.032),
    ("MI", "Michigan", 0.030),
    ("NJ", "New Jersey", 0.027),
    ("VA", "Virginia", 0.026),
    ("WA", "Washington", 0.023),
    ("AZ", "Arizona", 0.022),
    ("MA", "Massachusetts", 0.021),
    ("TN", "Tennessee", 0.021),
    ("IN", "Indiana", 0.020),
    ("MO", "Missouri", 0.018),
    ("MD", "Maryland", 0.018),
    ("WI", "Wisconsin", 0.018),
    ("CO", "Colorado", 0.018),
    ("MN", "Minnesota", 0.017),
    ("SC", "South Carolina", 0.016),
    ("AL", "Alabama", 0.015),
    ("LA", "Louisiana", 0.014),
    ("KY", "Kentucky", 0.014),
    ("OR", "Oregon", 0.013),
    ("OK", "Oklahoma", 0.012),
    ("CT", "Connecticut", 0.011),
    ("UT", "Utah", 0.010),
    ("NV", "Nevada", 0.010),
    ("AR", "Arkansas", 0.009),
    ("MS", "Mississippi", 0.009),
    ("KS", "Kansas", 0.009),
    ("NM", "New Mexico", 0.006),
    ("NE", "Nebraska", 0.006),
    ("ID", "Idaho", 0.006),
    ("WV", "West Virginia", 0.005),
    ("HI", "Hawaii", 0.004),
    ("NH", "New Hampshire", 0.004),
    ("ME", "Maine", 0.004),
    ("MT", "Montana", 0.003),
    ("RI", "Rhode Island", 0.003),
    ("DE", "Delaware", 0.003),
    ("SD", "South Dakota", 0.003),
    ("ND", "North Dakota", 0.002),
    ("AK", "Alaska", 0.002),
    ("VT", "Vermont", 0.002),
    ("WY", "Wyoming", 0.002),
]

# Cities by state (sample major cities)
CITIES_BY_STATE = {
    "CA": [
        "Los Angeles",
        "San Francisco",
        "San Diego",
        "San Jose",
        "Sacramento",
        "Oakland",
        "Fresno",
    ],
    "TX": ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth", "El Paso", "Arlington"],
    "FL": ["Miami", "Orlando", "Tampa", "Jacksonville", "Fort Lauderdale", "St. Petersburg"],
    "NY": ["New York", "Buffalo", "Rochester", "Yonkers", "Syracuse", "Albany"],
    "PA": ["Philadelphia", "Pittsburgh", "Allentown", "Erie", "Reading", "Scranton"],
    "IL": ["Chicago", "Aurora", "Naperville", "Joliet", "Rockford", "Springfield"],
    "OH": ["Columbus", "Cleveland", "Cincinnati", "Toledo", "Akron", "Dayton"],
    "GA": ["Atlanta", "Augusta", "Columbus", "Savannah", "Athens", "Macon"],
    "NC": ["Charlotte", "Raleigh", "Durham", "Greensboro", "Winston-Salem", "Fayetteville"],
    "MI": ["Detroit", "Grand Rapids", "Warren", "Sterling Heights", "Ann Arbor", "Lansing"],
}

# Street name components
STREET_NAMES = [
    "Main",
    "Oak",
    "Maple",
    "Cedar",
    "Pine",
    "Elm",
    "Washington",
    "Park",
    "Lake",
    "Hill",
    "River",
    "Valley",
    "Sunset",
    "Highland",
    "Forest",
    "Church",
    "Market",
    "Center",
    "Spring",
    "Mill",
    "North",
    "South",
    "East",
    "West",
    "Bridge",
    "School",
    "College",
    "Academy",
    "Franklin",
    "Lincoln",
    "Jefferson",
    "Madison",
    "Jackson",
    "Harrison",
    "Wilson",
]

STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Way", "Rd", "Ct", "Pl", "Cir"]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_address dimension table.

    Creates address records with:
    - address_id: Unique identifier
    - address_line1: Street address
    - address_line2: Apt/Suite (nullable)
    - city: City name
    - state_code: 2-letter state code
    - state_name: Full state name
    - postal_code: 5-digit ZIP code
    - country_code: ISO country code (US)
    - latitude: Approximate latitude (nullable)
    - longitude: Approximate longitude (nullable)
    - address_type: residential, commercial, or mixed

    The number of addresses is scaled to support customers and merchants.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale parameters.

    Returns:
        DataFrame with address dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Generate enough addresses for customers + merchants + some extra
    # Addresses can be shared (e.g., multiple people at same address)
    num_addresses = int((cfg.scale.customers + cfg.scale.merchants) * 1.2)

    # Prepare state sampling
    state_codes = [s[0] for s in US_STATES]
    state_names = {s[0]: s[1] for s in US_STATES}
    state_weights = np.array([s[2] for s in US_STATES])

    # Default cities for states not in our detailed list
    default_cities = ["Springfield", "Fairview", "Madison", "Georgetown", "Franklin"]

    rows = []
    for _ in range(num_addresses):
        address_id = ctx.stable_id("addr")

        # Sample state with population weights
        state_idx = rng.choice(len(state_codes), p=state_weights / sum(state_weights))
        state_code = state_codes[state_idx]
        state_name = state_names[state_code]

        # Sample city
        cities = CITIES_BY_STATE.get(state_code, default_cities)
        city = cities[rng.integers(0, len(cities))]

        # Generate street address
        street_num = rng.integers(1, 9999)
        street_name = STREET_NAMES[rng.integers(0, len(STREET_NAMES))]
        street_type = STREET_TYPES[rng.integers(0, len(STREET_TYPES))]
        address_line1 = f"{street_num} {street_name} {street_type}"

        # Sometimes add unit/apt number
        address_line2 = None
        if rng.random() < 0.2:
            unit_type = rng.choice(["Apt", "Suite", "Unit", "#"])
            unit_num = rng.integers(1, 500)
            address_line2 = f"{unit_type} {unit_num}"

        # Generate ZIP code (simplified - not geographically accurate)
        postal_code = f"{rng.integers(10000, 99999):05d}"

        # Address type distribution
        address_type = ctx.sample_categorical(
            rng,
            ["residential", "commercial", "mixed"],
            weights=[0.75, 0.20, 0.05],
        )

        # Approximate coordinates (simplified - not accurate)
        # US roughly: lat 25-48, lon -125 to -70
        latitude = round(float(rng.uniform(25.0, 48.0)), 6) if rng.random() > 0.1 else None
        longitude = round(float(rng.uniform(-125.0, -70.0)), 6) if latitude else None

        row = {
            "address_id": address_id,
            "address_line1": address_line1,
            "address_line2": address_line2,
            "city": city,
            "state_code": state_code,
            "state_name": state_name,
            "postal_code": postal_code,
            "country_code": "US",
            "latitude": latitude,
            "longitude": longitude,
            "address_type": address_type,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

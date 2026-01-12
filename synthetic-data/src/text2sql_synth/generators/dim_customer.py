"""Generator for dim_customer dimension table.

Generates customer records with realistic attributes and risk profiles.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "dim_customer"

# Sample name data
FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
    "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
]

RISK_TIERS = ["low", "medium", "high", "critical"]

CUSTOMER_SEGMENTS = ["retail", "premium", "business", "vip"]
CUSTOMER_SEGMENT_WEIGHTS = [0.60, 0.25, 0.12, 0.03]


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the dim_customer dimension table.

    Creates customer records with:
    - customer_id: Unique identifier
    - first_name: Customer first name
    - last_name: Customer last name
    - email: Customer email address
    - phone: Phone number (nullable)
    - date_of_birth: Birth date
    - customer_since: Account opening date
    - risk_tier: low, medium, high, critical
    - customer_segment: retail, premium, business, vip
    - is_active: Whether customer is active
    - activity_score: Long-tail distribution for transaction likelihood
    - primary_address_id: FK to dim_address (nullable initially)

    Long-tail activity_score ensures some customers are much more active.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with scale and distribution parameters.

    Returns:
        DataFrame with customer dimension data.
    """
    rng = ctx.rng_for(TABLE_NAME)
    num_customers = cfg.scale.customers

    # Get address IDs for FK assignment
    address_df = ctx.get_table("dim_address")
    if address_df is not None and len(address_df) > 0:
        address_ids = address_df["address_id"].tolist()
    else:
        address_ids = None

    rows = []
    for _ in range(num_customers):
        customer_id = ctx.stable_id("cust")

        # Names
        first_name = FIRST_NAMES[rng.integers(0, len(FIRST_NAMES))]
        last_name = LAST_NAMES[rng.integers(0, len(LAST_NAMES))]

        # Email (deterministic from name + id)
        email_domain = ctx.sample_categorical(
            rng,
            ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com", "icloud.com"],
            weights=[0.35, 0.20, 0.15, 0.10, 0.10, 0.10],
        )
        # Use customer_id suffix to ensure uniqueness
        id_suffix = customer_id.split("_")[1]
        email = f"{first_name.lower()}.{last_name.lower()}{id_suffix}@{email_domain}"

        # Phone (80% have phone)
        phone = None
        if rng.random() < 0.8:
            area_code = rng.integers(200, 999)
            exchange = rng.integers(200, 999)
            subscriber = rng.integers(1000, 9999)
            phone = f"+1{area_code}{exchange}{subscriber}"

        # Date of birth (18-85 years old from time window start)
        max_age_days = 85 * 365
        min_age_days = 18 * 365
        age_days = rng.integers(min_age_days, max_age_days)
        date_of_birth = cfg.time_window.start_date - timedelta(days=int(age_days))

        # Customer since (between 10 years ago and time window start)
        max_tenure_days = 10 * 365
        tenure_days = rng.integers(0, max_tenure_days)
        customer_since = cfg.time_window.start_date - timedelta(days=int(tenure_days))

        # Risk tier with configurable weights
        risk_tier = ctx.sample_categorical(
            rng,
            RISK_TIERS,
            weights=cfg.distribution.risk_tier_weights,
        )

        # Customer segment
        customer_segment = ctx.sample_categorical(
            rng,
            CUSTOMER_SEGMENTS,
            weights=CUSTOMER_SEGMENT_WEIGHTS,
        )

        # Active status (95% active)
        is_active = rng.random() < 0.95

        # Activity score: Pareto distribution for long-tail
        # Higher score = more likely to transact
        activity_score = round(
            float(ctx.sample_pareto(rng, cfg.distribution.long_tail_alpha, scale=1.0)),
            4
        )
        # Cap at 100 for sanity
        activity_score = min(activity_score, 100.0)

        # Assign primary address (if addresses exist)
        primary_address_id = None
        if address_ids:
            primary_address_id = address_ids[rng.integers(0, len(address_ids))]

        row = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "date_of_birth": date_of_birth,
            "customer_since": customer_since,
            "risk_tier": risk_tier,
            "customer_segment": customer_segment,
            "is_active": is_active,
            "activity_score": activity_score,
            "primary_address_id": primary_address_id,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

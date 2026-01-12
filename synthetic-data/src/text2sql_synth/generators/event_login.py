"""Generator for event_login event table.

Login events with device and location tracking.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "event_login"

# Login outcomes
LOGIN_OUTCOMES = ["success", "failed_password", "failed_mfa", "locked", "expired"]
LOGIN_OUTCOME_WEIGHTS = [0.92, 0.04, 0.02, 0.01, 0.01]

# Login channels
LOGIN_CHANNELS = ["mobile_app", "web", "api"]
LOGIN_CHANNEL_WEIGHTS = [0.55, 0.40, 0.05]

# Risk multipliers for failed logins
RISK_FAILURE_MULTIPLIERS = {
    "low": 0.5,
    "medium": 1.0,
    "high": 2.0,
    "critical": 3.0,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the event_login event table.

    Creates login event records with:
    - login_id: Unique identifier
    - customer_id: FK to dim_customer
    - account_id: FK to dim_account (nullable, some logins are app-level)
    - device_id: FK to event_device (nullable)
    - login_ts: Login timestamp
    - login_outcome: success, failed_password, failed_mfa, locked, expired
    - login_channel: mobile_app, web, api
    - ip_address: IP address used
    - location_country: Country code
    - location_city: City (nullable)
    - session_duration_seconds: Duration if successful (nullable)
    - mfa_method: MFA method used (nullable)

    Login frequency is based on customer activity score.
    Failed logins are more common for high-risk tiers.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration.

    Returns:
        DataFrame with login event data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get customer data
    customer_df = ctx.get_table("dim_customer")
    if customer_df is None or len(customer_df) == 0:
        raise ValueError("dim_customer must be generated before event_login")

    # Get account data
    account_df = ctx.get_table("dim_account")
    customer_accounts = account_df.groupby("customer_id")["account_id"].apply(list).to_dict()

    # Get device data
    device_df = ctx.get_table("event_device")
    if device_df is not None:
        customer_devices = device_df.groupby("customer_id")["device_id"].apply(list).to_dict()
    else:
        customer_devices = {}

    # Get time range
    time_df = ctx.get_table("dim_time")
    dates = sorted(time_df["full_date"].tolist())

    rows = []

    for _, cust_row in customer_df.iterrows():
        if not cust_row["is_active"]:
            continue

        customer_id = cust_row["customer_id"]
        risk_tier = cust_row["risk_tier"]
        activity_score = cust_row["activity_score"]

        # Number of logins based on activity score
        # Higher activity = more logins
        base_logins_per_day = 0.5 + activity_score * 0.3
        total_days = len(dates)
        expected_logins = int(base_logins_per_day * total_days)
        # Cap at reasonable number
        num_logins = min(expected_logins, total_days * 5)
        num_logins = max(1, num_logins)

        accounts = customer_accounts.get(customer_id, [])
        devices = customer_devices.get(customer_id, [])

        for _ in range(num_logins):
            login_id = ctx.stable_id("login")

            # Random date in range
            login_date = dates[rng.integers(0, len(dates))]
            # Random time (peak during business hours)
            hour_weights = np.array(
                [
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.01,
                    0.02,  # 0-5
                    0.04,
                    0.08,
                    0.10,
                    0.10,
                    0.08,
                    0.08,  # 6-11
                    0.06,
                    0.06,
                    0.06,
                    0.05,
                    0.05,
                    0.06,  # 12-17
                    0.07,
                    0.08,
                    0.06,
                    0.04,
                    0.02,
                    0.01,  # 18-23
                ]
            )
            hour = rng.choice(24, p=hour_weights / hour_weights.sum())
            minute = rng.integers(0, 60)
            second = rng.integers(0, 60)
            login_ts = datetime.combine(login_date, datetime.min.time()).replace(
                hour=int(hour), minute=int(minute), second=int(second)
            )

            # Account (nullable - some logins are app-level)
            account_id = None
            if accounts and rng.random() < 0.8:
                account_id = accounts[rng.integers(0, len(accounts))]

            # Device (nullable)
            device_id = None
            if devices and rng.random() < 0.9:
                device_id = devices[rng.integers(0, len(devices))]

            # Login outcome (risk-correlated failure rate)
            risk_multiplier = RISK_FAILURE_MULTIPLIERS.get(risk_tier, 1.0)
            failure_rate = 0.08 * risk_multiplier

            if rng.random() < failure_rate:
                login_outcome = ctx.sample_categorical(
                    rng,
                    ["failed_password", "failed_mfa", "locked", "expired"],
                    weights=[0.6, 0.25, 0.10, 0.05],
                )
            else:
                login_outcome = "success"

            # Channel
            login_channel = ctx.sample_categorical(
                rng, LOGIN_CHANNELS, weights=LOGIN_CHANNEL_WEIGHTS
            )

            # IP address (simplified)
            ip_address = (
                f"{rng.integers(1, 255)}.{rng.integers(0, 255)}."
                f"{rng.integers(0, 255)}.{rng.integers(1, 255)}"
            )

            # Location (mostly US)
            if rng.random() < 0.95:
                location_country = "US"
                location_city = ctx.sample_categorical(
                    rng,
                    ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", None],
                    weights=[0.15, 0.12, 0.10, 0.08, 0.05, 0.50],
                )
            else:
                location_country = ctx.sample_categorical(
                    rng, ["CA", "MX", "GB", "DE"], weights=[0.4, 0.3, 0.2, 0.1]
                )
                location_city = None

            # Session duration (only for successful logins)
            session_duration_seconds = None
            if login_outcome == "success":
                # Session 1 minute to 2 hours, Pareto distributed
                session_duration_seconds = int(min(ctx.sample_pareto(rng, 2.0, scale=60), 7200))

            # MFA method (for some logins)
            mfa_method = None
            if rng.random() < 0.6:
                mfa_method = ctx.sample_categorical(
                    rng,
                    ["sms", "totp", "push", "email"],
                    weights=[0.35, 0.30, 0.25, 0.10],
                )

            row = {
                "login_id": login_id,
                "customer_id": customer_id,
                "account_id": account_id,
                "device_id": device_id,
                "login_ts": login_ts,
                "login_outcome": login_outcome,
                "login_channel": login_channel,
                "ip_address": ip_address,
                "location_country": location_country,
                "location_city": location_city,
                "session_duration_seconds": session_duration_seconds,
                "mfa_method": mfa_method,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

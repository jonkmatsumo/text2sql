"""Generator for event_device event table.

Device records with emulator patterns correlated to risk tier.
"""

from __future__ import annotations

import pandas as pd
from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "event_device"

# Device types
DEVICE_TYPES = ["mobile_android", "mobile_ios", "tablet_android", "tablet_ios", "desktop", "web"]
DEVICE_TYPE_WEIGHTS = [0.35, 0.30, 0.08, 0.07, 0.10, 0.10]

# OS versions
ANDROID_VERSIONS = ["11", "12", "13", "14"]
IOS_VERSIONS = ["15.0", "16.0", "17.0", "17.1"]
DESKTOP_OS = ["Windows 10", "Windows 11", "macOS 13", "macOS 14", "Linux"]

# Device manufacturers
ANDROID_MANUFACTURERS = ["Samsung", "Google", "OnePlus", "Xiaomi", "Motorola"]
IOS_MANUFACTURERS = ["Apple"]

# Risk tier emulator rate multipliers
RISK_EMULATOR_MULTIPLIERS = {
    "low": 0.3,
    "medium": 1.0,
    "high": 3.0,
    "critical": 6.0,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the event_device event table.

    Creates device records with:
    - device_id: Unique identifier (matches device_id in transactions)
    - customer_id: FK to dim_customer
    - device_type: mobile_android, mobile_ios, tablet, desktop, web
    - device_manufacturer: Device manufacturer
    - device_model: Device model
    - os_version: Operating system version
    - app_version: Application version (if mobile)
    - is_emulator: Whether device is flagged as emulator
    - is_rooted: Whether device is rooted/jailbroken
    - first_seen_ts: When device was first seen
    - last_seen_ts: When device was last seen
    - risk_score: Device risk score (0-100)

    Device records are created from fact_transaction device_ids.
    Emulator rate is strongly correlated with customer risk tier.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration with emulator rate.

    Returns:
        DataFrame with device event data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get transaction data for device_ids
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None or len(transaction_df) == 0:
        raise ValueError("fact_transaction must be generated before event_device")

    # Get customer data for risk tiers
    customer_df = ctx.get_table("dim_customer")
    customer_risk = dict(zip(customer_df["customer_id"], customer_df["risk_tier"]))

    # Get unique devices from transactions
    devices_with_customers = transaction_df[transaction_df["device_id"].notna()][
        ["device_id", "customer_id", "transaction_ts", "is_emulator"]
    ].copy()

    # Aggregate to get first/last seen per device
    device_stats = (
        devices_with_customers.groupby("device_id")
        .agg(
            {
                "customer_id": "first",
                "transaction_ts": ["min", "max"],
                "is_emulator": "first",  # Take the emulator flag from first occurrence
            }
        )
        .reset_index()
    )

    device_stats.columns = [
        "device_id",
        "customer_id",
        "first_seen_ts",
        "last_seen_ts",
        "is_emulator",
    ]

    rows = []
    for _, device_row in device_stats.iterrows():
        device_id = device_row["device_id"]
        customer_id = device_row["customer_id"]
        risk_tier = customer_risk.get(customer_id, "low")

        # Device type
        device_type = ctx.sample_categorical(rng, DEVICE_TYPES, weights=DEVICE_TYPE_WEIGHTS)

        # Manufacturer and model based on type
        if "android" in device_type:
            manufacturer = ctx.sample_categorical(rng, ANDROID_MANUFACTURERS)
            model = f"{manufacturer[0]}{rng.integers(1, 20)}"
            os_version = f"Android {ctx.sample_categorical(rng, ANDROID_VERSIONS)}"
            app_version = f"3.{rng.integers(0, 10)}.{rng.integers(0, 100)}"
        elif "ios" in device_type:
            manufacturer = "Apple"
            if "tablet" in device_type:
                model = f"iPad{rng.integers(8, 12)}"
            else:
                model = f"iPhone{rng.integers(11, 16)}"
            os_version = f"iOS {ctx.sample_categorical(rng, IOS_VERSIONS)}"
            app_version = f"3.{rng.integers(0, 10)}.{rng.integers(0, 100)}"
        elif device_type == "desktop":
            manufacturer = ctx.sample_categorical(rng, ["Dell", "HP", "Lenovo", "Apple", "Custom"])
            model = f"{manufacturer} Desktop"
            os_version = ctx.sample_categorical(rng, DESKTOP_OS)
            app_version = None
        else:  # web
            manufacturer = ctx.sample_categorical(rng, ["Chrome", "Firefox", "Safari", "Edge"])
            model = f"{manufacturer} Browser"
            os_version = f"{manufacturer} {rng.integers(90, 120)}"
            app_version = None

        # Use the is_emulator flag from transactions (already risk-correlated)
        is_emulator = device_row["is_emulator"]

        # Rooted/jailbroken (correlated with emulator and risk)
        base_root_rate = 0.02
        if is_emulator:
            root_rate = 0.3  # Emulators often appear rooted
        else:
            root_rate = base_root_rate * RISK_EMULATOR_MULTIPLIERS.get(risk_tier, 1.0)
        is_rooted = rng.random() < root_rate

        # Risk score (0-100, correlated with risk tier and flags)
        base_score = {"low": 10, "medium": 30, "high": 50, "critical": 70}
        risk_score = base_score.get(risk_tier, 25)
        if is_emulator:
            risk_score += 20
        if is_rooted:
            risk_score += 15
        risk_score = min(100, risk_score + int(rng.integers(-10, 15)))

        row = {
            "device_id": device_id,
            "customer_id": customer_id,
            "device_type": device_type,
            "device_manufacturer": manufacturer,
            "device_model": model,
            "os_version": os_version,
            "app_version": app_version,
            "is_emulator": is_emulator,
            "is_rooted": is_rooted,
            "first_seen_ts": device_row["first_seen_ts"],
            "last_seen_ts": device_row["last_seen_ts"],
            "risk_score": risk_score,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df

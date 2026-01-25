"""Configuration schema for synthetic data generation."""

from __future__ import annotations

import json
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class ScalePreset(str, Enum):
    """Predefined scale presets for data generation."""

    SMALL = "small"
    MVP = "mvp"
    MEDIUM = "medium"


class ScaleConfig(BaseModel):
    """Row count configuration for data generation."""

    customers: int = Field(ge=1, description="Number of customer records")
    accounts_per_customer_min: int = Field(ge=1, description="Minimum accounts per customer")
    accounts_per_customer_max: int = Field(ge=1, description="Maximum accounts per customer")
    merchants: int = Field(ge=1, description="Number of merchant records")
    txns_per_day: int = Field(ge=1, description="Average transactions per day")
    cards_per_account_min: int = Field(ge=1, default=1, description="Minimum cards per account")
    cards_per_account_max: int = Field(ge=1, default=3, description="Maximum cards per account")

    @model_validator(mode="after")
    def validate_ranges(self) -> "ScaleConfig":
        """Ensure min <= max for all range fields."""
        if self.accounts_per_customer_min > self.accounts_per_customer_max:
            raise ValueError("accounts_per_customer_min must be <= accounts_per_customer_max")
        if self.cards_per_account_min > self.cards_per_account_max:
            raise ValueError("cards_per_account_min must be <= cards_per_account_max")
        return self


class DistributionConfig(BaseModel):
    """Distribution parameters for data generation."""

    seasonality_strength: float = Field(
        ge=0.0,
        le=1.0,
        default=0.3,
        description="Strength of seasonal patterns (0=none, 1=strong)",
    )
    long_tail_alpha: float = Field(
        gt=1.0,
        default=2.0,
        description="Zipf alpha for long-tail distributions (higher=steeper)",
    )
    risk_tier_weights: list[float] = Field(
        default=[0.7, 0.2, 0.08, 0.02],
        description="Weights for risk tiers [low, medium, high, critical]",
    )
    transaction_amount_pareto_alpha: float = Field(
        gt=0.0,
        default=1.5,
        description="Pareto alpha for transaction amounts",
    )
    merchant_popularity_zipf_alpha: float = Field(
        gt=1.0,
        default=1.5,
        description="Zipf alpha for merchant popularity distribution",
    )

    @field_validator("risk_tier_weights")
    @classmethod
    def validate_risk_weights(cls, v: list[float]) -> list[float]:
        """Ensure risk tier weights are valid probabilities."""
        if len(v) != 4:
            raise ValueError("risk_tier_weights must have exactly 4 values")
        if any(w < 0 for w in v):
            raise ValueError("risk_tier_weights must all be non-negative")
        return v


class RateConfig(BaseModel):
    """Rate configuration for various event types."""

    dispute_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.008,
        description="Fraction of transactions that become disputes",
    )
    refund_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.02,
        description="Fraction of transactions that are refunded",
    )
    decline_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.03,
        description="Fraction of transactions that are declined",
    )
    fraud_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.001,
        description="Fraction of transactions that are fraudulent",
    )
    emulator_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.05,
        description="Fraction of devices flagged as emulators",
    )
    freeze_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.002,
        description="Fraction of accounts that get frozen",
    )
    chargeback_rate: float = Field(
        ge=0.0,
        le=1.0,
        default=0.005,
        description="Fraction of disputes that become chargebacks",
    )


class OutputConfig(BaseModel):
    """Output format and manifest options."""

    csv: bool = Field(default=True, description="Output CSV files")
    parquet: bool = Field(default=True, description="Output Parquet files")
    include_row_hashes: bool = Field(
        default=True,
        description="Include row-level hashes in output for verification",
    )
    include_file_hashes: bool = Field(
        default=True,
        description="Include file hashes in manifest",
    )
    compression: Literal["none", "gzip", "snappy"] = Field(
        default="snappy",
        description="Compression for Parquet files",
    )


class TimeWindowConfig(BaseModel):
    """Time window for data generation."""

    start_date: date = Field(description="Start date for generated data (inclusive)")
    end_date: date = Field(description="End date for generated data (inclusive)")

    @model_validator(mode="after")
    def validate_date_range(self) -> "TimeWindowConfig":
        """Ensure start_date <= end_date."""
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self

    @property
    def days(self) -> int:
        """Number of days in the time window."""
        return (self.end_date - self.start_date).days + 1


class SynthConfig(BaseModel):
    """Main configuration for synthetic data generation."""

    seed: int = Field(ge=0, description="Random seed for reproducible generation")
    scale: ScaleConfig = Field(description="Row count configuration")
    time_window: TimeWindowConfig = Field(description="Date range for generated data")
    distribution: DistributionConfig = Field(
        default_factory=DistributionConfig,
        description="Distribution parameters",
    )
    rates: RateConfig = Field(
        default_factory=RateConfig,
        description="Event rate configuration",
    )
    output: OutputConfig = Field(
        default_factory=OutputConfig,
        description="Output format options",
    )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "SynthConfig":
        """Load configuration from a YAML file.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            Parsed SynthConfig instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the YAML is invalid.
        """
        import yaml

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data)

    @classmethod
    def from_json(cls, path: str | Path) -> "SynthConfig":
        """Load configuration from a JSON file.

        Args:
            path: Path to the JSON configuration file.

        Returns:
            Parsed SynthConfig instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the JSON is invalid.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path) as f:
            data = json.load(f)

        return cls.model_validate(data)

    @classmethod
    def preset(cls, name: str | ScalePreset) -> "SynthConfig":
        """Get a built-in configuration preset.

        Args:
            name: Preset name ("small", "mvp", or "medium").

        Returns:
            SynthConfig with preset values.

        Raises:
            ValueError: If the preset name is unknown.
        """
        if isinstance(name, str):
            try:
                name = ScalePreset(name.lower())
            except ValueError:
                valid = [p.value for p in ScalePreset]
                raise ValueError(f"Unknown preset '{name}'. Valid presets: {valid}")

        presets = {
            ScalePreset.SMALL: cls._preset_small(),
            ScalePreset.MVP: cls._preset_mvp(),
            ScalePreset.MEDIUM: cls._preset_medium(),
        }

        return presets[name]

    @classmethod
    def _preset_small(cls) -> "SynthConfig":
        """Small preset for quick testing."""
        return cls(
            seed=42,
            scale=ScaleConfig(
                customers=100,
                accounts_per_customer_min=1,
                accounts_per_customer_max=2,
                merchants=50,
                txns_per_day=200,
                cards_per_account_min=1,
                cards_per_account_max=2,
            ),
            time_window=TimeWindowConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 7),
            ),
            distribution=DistributionConfig(
                seasonality_strength=0.2,
                long_tail_alpha=2.0,
            ),
            rates=RateConfig(
                dispute_rate=0.01,
                refund_rate=0.02,
                decline_rate=0.03,
                fraud_rate=0.002,
            ),
            output=OutputConfig(
                csv=True,
                parquet=True,
                include_row_hashes=True,
                include_file_hashes=True,
            ),
        )

    @classmethod
    def _preset_mvp(cls) -> "SynthConfig":
        """MVP preset for development and integration testing."""
        return cls(
            seed=42,
            scale=ScaleConfig(
                customers=1000,
                accounts_per_customer_min=1,
                accounts_per_customer_max=3,
                merchants=200,
                txns_per_day=2000,
                cards_per_account_min=1,
                cards_per_account_max=3,
            ),
            time_window=TimeWindowConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            ),
            distribution=DistributionConfig(
                seasonality_strength=0.3,
                long_tail_alpha=2.0,
                risk_tier_weights=[0.7, 0.2, 0.08, 0.02],
            ),
            rates=RateConfig(
                dispute_rate=0.008,
                refund_rate=0.02,
                decline_rate=0.03,
                fraud_rate=0.001,
                emulator_rate=0.05,
                freeze_rate=0.002,
                chargeback_rate=0.005,
            ),
            output=OutputConfig(
                csv=True,
                parquet=True,
                include_row_hashes=True,
                include_file_hashes=True,
            ),
        )

    @classmethod
    def _preset_medium(cls) -> "SynthConfig":
        """Medium preset for more realistic testing scenarios."""
        return cls(
            seed=42,
            scale=ScaleConfig(
                customers=10000,
                accounts_per_customer_min=1,
                accounts_per_customer_max=4,
                merchants=1000,
                txns_per_day=20000,
                cards_per_account_min=1,
                cards_per_account_max=4,
            ),
            time_window=TimeWindowConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 3, 31),
            ),
            distribution=DistributionConfig(
                seasonality_strength=0.4,
                long_tail_alpha=1.8,
                risk_tier_weights=[0.65, 0.22, 0.10, 0.03],
                transaction_amount_pareto_alpha=1.3,
                merchant_popularity_zipf_alpha=1.4,
            ),
            rates=RateConfig(
                dispute_rate=0.008,
                refund_rate=0.025,
                decline_rate=0.035,
                fraud_rate=0.0015,
                emulator_rate=0.04,
                freeze_rate=0.003,
                chargeback_rate=0.006,
            ),
            output=OutputConfig(
                csv=True,
                parquet=True,
                include_row_hashes=True,
                include_file_hashes=True,
                compression="snappy",
            ),
        )

    def to_json(self, indent: int = 2) -> str:
        """Serialize configuration to JSON string.

        Args:
            indent: Number of spaces for indentation.

        Returns:
            JSON string representation.
        """
        return self.model_dump_json(indent=indent)

    def to_dict(self) -> dict:
        """Convert configuration to dictionary.

        Returns:
            Dictionary representation with date objects converted to ISO strings.
        """
        return self.model_dump(mode="json")

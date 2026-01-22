"""Tests for configuration schema and loading.

NOTE:
Renamed from test_config.py to avoid pytest import collisions with other
test_config.py modules in the monorepo when running tests from repo root.
"""

import json
import tempfile
from datetime import date
from pathlib import Path

import pytest
import yaml

from text2sql_synth.config import (
    DistributionConfig,
    ScaleConfig,
    ScalePreset,
    SynthConfig,
    TimeWindowConfig,
)


class TestScaleConfig:
    """Tests for ScaleConfig validation."""

    def test_valid_config(self) -> None:
        """Valid scale config is accepted."""
        config = ScaleConfig(
            customers=100,
            accounts_per_customer_min=1,
            accounts_per_customer_max=3,
            merchants=50,
            txns_per_day=200,
        )
        assert config.customers == 100

    def test_min_greater_than_max_fails(self) -> None:
        """accounts_per_customer_min > max raises error."""
        with pytest.raises(ValueError, match="accounts_per_customer_min"):
            ScaleConfig(
                customers=100,
                accounts_per_customer_min=5,
                accounts_per_customer_max=2,
                merchants=50,
                txns_per_day=200,
            )

    def test_cards_min_greater_than_max_fails(self) -> None:
        """cards_per_account_min > max raises error."""
        with pytest.raises(ValueError, match="cards_per_account_min"):
            ScaleConfig(
                customers=100,
                accounts_per_customer_min=1,
                accounts_per_customer_max=2,
                merchants=50,
                txns_per_day=200,
                cards_per_account_min=5,
                cards_per_account_max=2,
            )


class TestDistributionConfig:
    """Tests for DistributionConfig validation."""

    def test_defaults(self) -> None:
        """Default values are applied."""
        config = DistributionConfig()
        assert config.seasonality_strength == 0.3
        assert config.long_tail_alpha == 2.0
        assert len(config.risk_tier_weights) == 4

    def test_invalid_risk_weights_count(self) -> None:
        """risk_tier_weights must have exactly 4 values."""
        with pytest.raises(ValueError, match="exactly 4 values"):
            DistributionConfig(risk_tier_weights=[0.5, 0.5])

    def test_negative_risk_weights(self) -> None:
        """risk_tier_weights must be non-negative."""
        with pytest.raises(ValueError, match="non-negative"):
            DistributionConfig(risk_tier_weights=[0.7, -0.1, 0.2, 0.2])


class TestTimeWindowConfig:
    """Tests for TimeWindowConfig validation."""

    def test_valid_range(self) -> None:
        """Valid date range is accepted."""
        config = TimeWindowConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
        assert config.days == 31

    def test_single_day(self) -> None:
        """Single day range has 1 day."""
        config = TimeWindowConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
        )
        assert config.days == 1

    def test_start_after_end_fails(self) -> None:
        """start_date > end_date raises error."""
        with pytest.raises(ValueError, match="start_date must be <= end_date"):
            TimeWindowConfig(
                start_date=date(2024, 2, 1),
                end_date=date(2024, 1, 1),
            )


class TestPresets:
    """Tests for built-in configuration presets."""

    def test_preset_small(self) -> None:
        """Small preset loads correctly."""
        config = SynthConfig.preset("small")
        assert config.seed == 42
        assert config.scale.customers == 100
        assert config.scale.merchants == 50
        assert config.time_window.days == 7

    def test_preset_mvp(self) -> None:
        """MVP preset loads correctly."""
        config = SynthConfig.preset("mvp")
        assert config.seed == 42
        assert config.scale.customers == 1000
        assert config.scale.merchants == 200
        assert config.time_window.days == 31

    def test_preset_medium(self) -> None:
        """Medium preset loads correctly."""
        config = SynthConfig.preset("medium")
        assert config.seed == 42
        assert config.scale.customers == 10000
        assert config.scale.merchants == 1000
        assert config.time_window.days == 91

    def test_preset_enum(self) -> None:
        """Preset works with enum value."""
        config = SynthConfig.preset(ScalePreset.SMALL)
        assert config.scale.customers == 100

    def test_preset_case_insensitive(self) -> None:
        """Preset name is case-insensitive."""
        config = SynthConfig.preset("SMALL")
        assert config.scale.customers == 100

    def test_unknown_preset_fails(self) -> None:
        """Unknown preset raises ValueError."""
        with pytest.raises(ValueError, match="Unknown preset"):
            SynthConfig.preset("unknown")


class TestYamlLoading:
    """Tests for YAML configuration loading."""

    def test_load_yaml(self) -> None:
        """YAML config loads correctly."""
        config_data = {
            "seed": 123,
            "scale": {
                "customers": 500,
                "accounts_per_customer_min": 1,
                "accounts_per_customer_max": 2,
                "merchants": 100,
                "txns_per_day": 1000,
            },
            "time_window": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-15",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config_data, f)
            temp_path = f.name

        try:
            config = SynthConfig.from_yaml(temp_path)
            assert config.seed == 123
            assert config.scale.customers == 500
            assert config.time_window.start_date == date(
                2024, 1, 15
            ) or config.time_window.start_date == date(2024, 1, 1)
        finally:
            Path(temp_path).unlink()

    def test_load_yaml_missing_file(self) -> None:
        """Missing YAML file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            SynthConfig.from_yaml("/nonexistent/path.yaml")


class TestJsonLoading:
    """Tests for JSON configuration loading."""

    def test_load_json(self) -> None:
        """JSON config loads correctly."""
        config_data = {
            "seed": 456,
            "scale": {
                "customers": 300,
                "accounts_per_customer_min": 1,
                "accounts_per_customer_max": 2,
                "merchants": 75,
                "txns_per_day": 500,
            },
            "time_window": {
                "start_date": "2024-02-01",
                "end_date": "2024-02-28",
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            config = SynthConfig.from_json(temp_path)
            assert config.seed == 456
            assert config.scale.customers == 300
        finally:
            Path(temp_path).unlink()

    def test_load_json_missing_file(self) -> None:
        """Missing JSON file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            SynthConfig.from_json("/nonexistent/path.json")


class TestSerialization:
    """Tests for configuration serialization."""

    def test_to_json(self) -> None:
        """to_json produces valid JSON."""
        config = SynthConfig.preset("small")
        json_str = config.to_json()
        parsed = json.loads(json_str)
        assert parsed["seed"] == 42
        assert parsed["scale"]["customers"] == 100

    def test_to_dict(self) -> None:
        """to_dict produces serializable dict."""
        config = SynthConfig.preset("small")
        d = config.to_dict()
        assert d["seed"] == 42
        # Dates should be ISO strings
        assert d["time_window"]["start_date"] == "2024-01-01"

    def test_roundtrip(self) -> None:
        """Config survives JSON roundtrip."""
        original = SynthConfig.preset("mvp")
        json_str = original.to_json()
        parsed = json.loads(json_str)
        restored = SynthConfig.model_validate(parsed)

        assert restored.seed == original.seed
        assert restored.scale.customers == original.scale.customers
        assert restored.time_window.start_date == original.time_window.start_date


class TestExampleConfigs:
    """Tests that example config files are valid."""

    @pytest.fixture
    def examples_dir(self) -> Path:
        """Path to example configurations."""
        return Path(__file__).parent / "examples"

    def test_config_small_yaml(self, examples_dir: Path) -> None:
        """config_small.yaml is valid."""
        config = SynthConfig.from_yaml(examples_dir / "config_small.yaml")
        assert config.scale.customers == 100

    def test_config_mvp_yaml(self, examples_dir: Path) -> None:
        """config_mvp.yaml is valid."""
        config = SynthConfig.from_yaml(examples_dir / "config_mvp.yaml")
        assert config.scale.customers == 1000

    def test_config_medium_yaml(self, examples_dir: Path) -> None:
        """config_medium.yaml is valid."""
        config = SynthConfig.from_yaml(examples_dir / "config_medium.yaml")
        assert config.scale.customers == 10000

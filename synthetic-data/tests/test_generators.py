"""Tests for dimension and bridge table generators."""

from datetime import date

import pandas as pd
import pytest

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext
from text2sql_synth.context import TimeWindowConfig as CtxTimeWindowConfig
from text2sql_synth.generators import (
    generate_bridge_customer_address,
    generate_dim_account,
    generate_dim_address,
    generate_dim_counterparty,
    generate_dim_customer,
    generate_dim_customer_scd2,
    generate_dim_institution,
    generate_dim_merchant,
    generate_dim_time,
)
from text2sql_synth.util.hashing import stable_hash_str


@pytest.fixture
def small_config() -> SynthConfig:
    """Get the small preset configuration."""
    return SynthConfig.preset("small")


@pytest.fixture
def context(small_config: SynthConfig) -> GenerationContext:
    """Create a generation context with small config."""
    time_window = CtxTimeWindowConfig(
        start_date=small_config.time_window.start_date,
        end_date=small_config.time_window.end_date,
    )
    return GenerationContext(
        seed=small_config.seed,
        schema_definition="test_schema",
        time_window=time_window,
    )


def generate_all_dimensions(ctx: GenerationContext, cfg: SynthConfig) -> dict[str, pd.DataFrame]:
    """Generate all dimension tables in correct dependency order."""
    results = {}

    # No dependencies
    results["dim_time"] = generate_dim_time(ctx, cfg)
    results["dim_institution"] = generate_dim_institution(ctx, cfg)
    results["dim_address"] = generate_dim_address(ctx, cfg)

    # Depends on dim_address, dim_institution
    results["dim_customer"] = generate_dim_customer(ctx, cfg)
    results["dim_merchant"] = generate_dim_merchant(ctx, cfg)

    # Depends on dim_customer, dim_institution
    results["dim_account"] = generate_dim_account(ctx, cfg)

    # Depends on dim_customer, dim_address
    results["bridge_customer_address"] = generate_bridge_customer_address(ctx, cfg)

    # Depends on dim_merchant
    results["dim_counterparty"] = generate_dim_counterparty(ctx, cfg)

    # Depends on dim_customer
    results["dim_customer_scd2"] = generate_dim_customer_scd2(ctx, cfg)

    return results


class TestDimTime:
    """Tests for dim_time generator."""

    def test_row_count_matches_date_range(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_time has one row per day in time window."""
        df = generate_dim_time(context, small_config)
        expected_days = small_config.time_window.days
        assert len(df) == expected_days

    def test_required_columns_exist(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_time has all required columns."""
        df = generate_dim_time(context, small_config)
        required_columns = [
            "date_key", "full_date", "day_of_week", "day_name",
            "day_of_month", "day_of_year", "week_of_year", "month",
            "month_name", "quarter", "year", "is_weekend", "is_holiday",
            "seasonality_factor",
        ]
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_date_key_format(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """date_key is in YYYYMMDD format."""
        df = generate_dim_time(context, small_config)
        first_row = df.iloc[0]
        assert first_row["date_key"] == 20240101


class TestDimInstitution:
    """Tests for dim_institution generator."""

    def test_has_all_institution_types(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_institution includes all institution types."""
        df = generate_dim_institution(context, small_config)
        types = set(df["institution_type"])
        assert "issuer" in types
        assert "acquirer" in types
        assert "processor" in types
        assert "network" in types

    def test_unique_institution_ids(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """institution_id values are unique."""
        df = generate_dim_institution(context, small_config)
        assert df["institution_id"].nunique() == len(df)


class TestDimAddress:
    """Tests for dim_address generator."""

    def test_row_count_scaled(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_address row count is scaled from config."""
        df = generate_dim_address(context, small_config)
        expected = int((small_config.scale.customers + small_config.scale.merchants) * 1.2)
        assert len(df) == expected

    def test_required_columns(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_address has required columns."""
        df = generate_dim_address(context, small_config)
        required = ["address_id", "city", "state_code", "postal_code", "country_code"]
        for col in required:
            assert col in df.columns


class TestDimCustomer:
    """Tests for dim_customer generator."""

    def test_row_count_matches_config(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_customer row count matches config.scale.customers."""
        # Need dim_address first
        generate_dim_address(context, small_config)
        df = generate_dim_customer(context, small_config)
        assert len(df) == small_config.scale.customers

    def test_risk_tiers_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """risk_tier values are valid."""
        generate_dim_address(context, small_config)
        df = generate_dim_customer(context, small_config)
        valid_tiers = {"low", "medium", "high", "critical"}
        assert set(df["risk_tier"]).issubset(valid_tiers)

    def test_activity_score_long_tail(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """activity_score shows long-tail distribution."""
        generate_dim_address(context, small_config)
        df = generate_dim_customer(context, small_config)
        # Most values should be low, with some high outliers
        median = df["activity_score"].median()
        max_val = df["activity_score"].max()
        # Max should be significantly higher than median (long tail)
        assert max_val > median * 2


class TestDimAccount:
    """Tests for dim_account generator."""

    def test_fk_customer_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """account.customer_id references valid customer_id."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        customer_df = generate_dim_customer(context, small_config)
        account_df = generate_dim_account(context, small_config)

        customer_ids = set(customer_df["customer_id"])
        account_customer_ids = set(account_df["customer_id"])
        assert account_customer_ids.issubset(customer_ids)

    def test_fk_institution_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """account.institution_id references valid issuer institution_id."""
        institution_df = generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        generate_dim_customer(context, small_config)
        account_df = generate_dim_account(context, small_config)

        issuer_ids = set(
            institution_df[institution_df["institution_type"] == "issuer"]["institution_id"]
        )
        account_institution_ids = set(account_df["institution_id"])
        assert account_institution_ids.issubset(issuer_ids)

    def test_accounts_per_customer_in_range(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """Each customer has accounts within configured range."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        generate_dim_customer(context, small_config)
        account_df = generate_dim_account(context, small_config)

        accounts_per_customer = account_df.groupby("customer_id").size()
        min_accounts = small_config.scale.accounts_per_customer_min
        max_accounts = small_config.scale.accounts_per_customer_max

        assert accounts_per_customer.min() >= min_accounts
        assert accounts_per_customer.max() <= max_accounts


class TestDimMerchant:
    """Tests for dim_merchant generator."""

    def test_row_count_matches_config(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """dim_merchant row count matches config.scale.merchants."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        df = generate_dim_merchant(context, small_config)
        assert len(df) == small_config.scale.merchants

    def test_fk_address_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """merchant.address_id references valid address_id."""
        generate_dim_institution(context, small_config)
        address_df = generate_dim_address(context, small_config)
        merchant_df = generate_dim_merchant(context, small_config)

        address_ids = set(address_df["address_id"])
        merchant_address_ids = set(merchant_df["address_id"].dropna())
        assert merchant_address_ids.issubset(address_ids)

    def test_popularity_score_long_tail(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """popularity_score shows Zipf-like distribution."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        df = generate_dim_merchant(context, small_config)

        median = df["popularity_score"].median()
        max_val = df["popularity_score"].max()
        # Long-tail: max >> median
        assert max_val > median


class TestBridgeCustomerAddress:
    """Tests for bridge_customer_address generator."""

    def test_fk_customer_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """bridge.customer_id references valid customer_id."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        customer_df = generate_dim_customer(context, small_config)
        bridge_df = generate_bridge_customer_address(context, small_config)

        customer_ids = set(customer_df["customer_id"])
        bridge_customer_ids = set(bridge_df["customer_id"])
        assert bridge_customer_ids.issubset(customer_ids)

    def test_fk_address_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """bridge.address_id references valid address_id."""
        generate_dim_institution(context, small_config)
        address_df = generate_dim_address(context, small_config)
        generate_dim_customer(context, small_config)
        bridge_df = generate_bridge_customer_address(context, small_config)

        address_ids = set(address_df["address_id"])
        bridge_address_ids = set(bridge_df["address_id"].dropna())
        assert bridge_address_ids.issubset(address_ids)

    def test_every_customer_has_primary(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """Every customer with address has a primary address entry."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        customer_df = generate_dim_customer(context, small_config)
        bridge_df = generate_bridge_customer_address(context, small_config)

        # Customers with primary address should have bridge entry
        customers_with_addr = set(
            customer_df[customer_df["primary_address_id"].notna()]["customer_id"]
        )
        primary_entries = set(
            bridge_df[bridge_df["address_type"] == "primary"]["customer_id"]
        )
        assert primary_entries == customers_with_addr


class TestDimCounterparty:
    """Tests for dim_counterparty generator."""

    def test_all_merchants_have_counterparty(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """Every merchant has a corresponding counterparty."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        merchant_df = generate_dim_merchant(context, small_config)
        counterparty_df = generate_dim_counterparty(context, small_config)

        merchant_counterparties = counterparty_df[
            counterparty_df["counterparty_type"] == "merchant"
        ]
        assert len(merchant_counterparties) == len(merchant_df)

    def test_fk_merchant_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """counterparty.merchant_id references valid merchant_id."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        merchant_df = generate_dim_merchant(context, small_config)
        counterparty_df = generate_dim_counterparty(context, small_config)

        merchant_ids = set(merchant_df["merchant_id"])
        counterparty_merchant_ids = set(counterparty_df["merchant_id"].dropna())
        assert counterparty_merchant_ids.issubset(merchant_ids)


class TestDimCustomerScd2:
    """Tests for dim_customer_scd2 generator."""

    def test_every_customer_has_current_version(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """Every customer has exactly one current version."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        customer_df = generate_dim_customer(context, small_config)
        scd2_df = generate_dim_customer_scd2(context, small_config)

        current_versions = scd2_df[scd2_df["is_current"] == True]
        assert len(current_versions) == len(customer_df)

    def test_fk_customer_id_valid(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """scd2.customer_id references valid customer_id."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        customer_df = generate_dim_customer(context, small_config)
        scd2_df = generate_dim_customer_scd2(context, small_config)

        customer_ids = set(customer_df["customer_id"])
        scd2_customer_ids = set(scd2_df["customer_id"])
        assert scd2_customer_ids == customer_ids

    def test_version_numbers_sequential(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """Version numbers are sequential for each customer."""
        generate_dim_institution(context, small_config)
        generate_dim_address(context, small_config)
        generate_dim_customer(context, small_config)
        scd2_df = generate_dim_customer_scd2(context, small_config)

        for customer_id in scd2_df["customer_id"].unique():
            versions = sorted(
                scd2_df[scd2_df["customer_id"] == customer_id]["version_number"]
            )
            expected = list(range(1, len(versions) + 1))
            assert versions == expected


class TestDeterminism:
    """Tests for deterministic output generation."""

    def test_same_seed_same_output(self, small_config: SynthConfig) -> None:
        """Same seed produces identical output."""
        time_window = CtxTimeWindowConfig(
            start_date=small_config.time_window.start_date,
            end_date=small_config.time_window.end_date,
        )

        # First run
        ctx1 = GenerationContext(
            seed=small_config.seed,
            schema_definition="test_schema",
            time_window=time_window,
        )
        results1 = generate_all_dimensions(ctx1, small_config)

        # Second run with same seed
        ctx2 = GenerationContext(
            seed=small_config.seed,
            schema_definition="test_schema",
            time_window=time_window,
        )
        results2 = generate_all_dimensions(ctx2, small_config)

        # Compare hashes of each table
        for table_name in results1:
            hash1 = stable_hash_str(results1[table_name].to_csv())
            hash2 = stable_hash_str(results2[table_name].to_csv())
            assert hash1 == hash2, f"Table {table_name} differs between runs"

    def test_different_seed_different_output(self, small_config: SynthConfig) -> None:
        """Different seeds produce different output."""
        time_window = CtxTimeWindowConfig(
            start_date=small_config.time_window.start_date,
            end_date=small_config.time_window.end_date,
        )

        # First run with seed 42
        ctx1 = GenerationContext(
            seed=42,
            schema_definition="test_schema",
            time_window=time_window,
        )
        results1 = generate_all_dimensions(ctx1, small_config)

        # Second run with seed 123
        ctx2 = GenerationContext(
            seed=123,
            schema_definition="test_schema",
            time_window=time_window,
        )
        results2 = generate_all_dimensions(ctx2, small_config)

        # At least dim_customer should differ (has random elements)
        hash1 = stable_hash_str(results1["dim_customer"].to_csv())
        hash2 = stable_hash_str(results2["dim_customer"].to_csv())
        assert hash1 != hash2


class TestTableRegistration:
    """Tests for table registration in context."""

    def test_all_tables_registered(
        self, context: GenerationContext, small_config: SynthConfig
    ) -> None:
        """All generated tables are registered in context."""
        generate_all_dimensions(context, small_config)

        expected_tables = [
            "dim_time", "dim_institution", "dim_address", "dim_customer",
            "dim_merchant", "dim_account", "bridge_customer_address",
            "dim_counterparty", "dim_customer_scd2",
        ]
        for table_name in expected_tables:
            assert context.get_table(table_name) is not None, f"Table {table_name} not registered"

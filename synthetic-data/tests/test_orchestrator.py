import pandas as pd
import pytest
from text2sql_synth import schema
from text2sql_synth.config import SynthConfig
from text2sql_synth.orchestrator import generate_all, generate_tables


def test_generate_full_dataset():
    """Verify that generate_all creates all expected tables for a small preset."""
    cfg = SynthConfig.preset("small")
    ctx, tables = generate_all(cfg)

    # Check that all tables in GENERATION_ORDER are present
    assert set(tables.keys()) == set(schema.GENERATION_ORDER)

    # Verify each table is a non-empty DataFrame (except maybe those with no data by chance,
    # but with small preset we expect data)
    for name, df in tables.items():
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0, f"Table {name} is empty"

        # Verify columns match schema
        expected_cols = set(schema.EXPECTED_COLUMNS[name])
        assert expected_cols.issubset(
            set(df.columns)
        ), f"Table {name} missing columns. Expected {expected_cols}, got {df.columns}"


def test_generate_partial_with_dependencies():
    """Verify that generating a specific table also pulls in its dependencies."""
    cfg = SynthConfig.preset("small")

    # Generate only fact_dispute
    # fact_dispute depends on fact_transaction
    # fact_transaction depends on many dimensions
    ctx, tables = generate_tables(cfg, only=["fact_dispute"])

    # Expected tables include fact_dispute and its transitive dependencies
    expected_tables = {
        "fact_dispute",
        "fact_transaction",
        "dim_time",
        "dim_customer",
        "dim_account",
        "dim_merchant",
        "dim_counterparty",
        "dim_institution",
        "dim_address",  # dim_customer and dim_merchant depend on address
    }

    for table in expected_tables:
        assert table in tables, f"Expected table {table} not generated"

    # Verify fact_refund is NOT generated.
    # It depends on fact_transaction but fact_dispute doesn't depend on it.
    assert "fact_refund" not in tables


def test_column_validation_error(monkeypatch):
    """Verify that missing columns trigger a ValueError."""
    cfg = SynthConfig.preset("small")

    # Mock a generator to return a DataFrame with missing columns
    from text2sql_synth import generators

    def mock_generate_dim_time(ctx, cfg):
        return pd.DataFrame({"wrong_column": [1, 2, 3]})

    monkeypatch.setattr(generators, "generate_dim_time", mock_generate_dim_time)

    with pytest.raises(ValueError, match="Table 'dim_time' is missing expected columns"):
        generate_tables(cfg, only=["dim_time"])


def test_resolve_dependencies_logic():
    """Directly test the dependency resolution logic."""
    from text2sql_synth.orchestrator import _resolve_dependencies

    # Test None (all tables)
    assert _resolve_dependencies(None) == set(schema.GENERATION_ORDER)

    # Test specific table
    deps = _resolve_dependencies(["fact_payment"])
    assert "fact_payment" in deps
    assert "fact_transaction" in deps
    assert "dim_customer" in deps
    # ... and so on

    # Test circular/redundant (not circular in our case but for robustness)
    deps = _resolve_dependencies(["dim_customer", "dim_address"])
    assert "dim_customer" in deps
    assert "dim_address" in deps
